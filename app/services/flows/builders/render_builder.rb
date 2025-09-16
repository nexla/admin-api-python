module Flows
  module Builders

    class RenderBuilder
      # @param accessible_options [Hash] options for filtering accessible resources. This will be passed to accessible_by_user. Options are:
      #  - :access_role [Symbol] access role to filter by. Default is :all.
      #  - :access_roles_only [Boolean] if true, only access roles will be loaded, without resources. Default is false.
      # @param sort_opts [Hash] options for rendering flows. Options are:
      #  - :sort_by [String] sort by field. Default is 'id'.
      #  - :sort_order [String] sort order. Default is 'desc'.
      def initialize(api_user_info, origin_nodes, accessible_options = {}, flows_only = false, sort_opts = {})
        @api_user_info = api_user_info
        @origin_nodes = origin_nodes
        @flows_only = flows_only
        @accessible_options = accessible_options.dup
        @sort_opts = sort_opts
        @linked_flows = nil
        @linked_flows_map = nil
      end

      def build
        resources = nil
        projects  = []

        origin_node_ids = origin_nodes.map(&:id)
        if origin_node_ids.blank?
          empty_flow =  FlowNode.empty_flow
          empty_flow.delete(:flow)
          return {
            flows: [],
            resources: empty_flow,
            projects: [],
            triggered_flows: [],
            triggering_flows: []
          }
        end

        children_map = {}

        origin_nodes = FlowNode.where(id: origin_node_ids)
                               .preload(:data_source)
                               .to_a
        child_nodes = FlowNode.where(origin_node_id: origin_node_ids).to_a

        shared_origins = FlowNode.where(shared_origin_node_id: origin_node_ids)
        shared_origins.each do |fn|
          if (fn.org_id == api_user_info.org&.id && fn.has_collaborator_access?(api_user_info.user)) || api_user_info.user.super_user?
            child_nodes << fn
          end
        end

        child_nodes.each do |fn|
          next if fn.parent_node_id.nil?
          children_map[fn.parent_node_id] ||= []
          children_map[fn.parent_node_id] << fn
        end

        triggered_flows = get_triggered_flows(origin_nodes)
        triggering_flows = get_triggering_flows(origin_nodes)
        @linked_flows, @linked_flows_map, linked_to_rag_flows = get_linked_flows(origin_nodes)

        unless flows_only
          flow_nodes_for_projects = origin_nodes + triggered_flows + triggering_flows + linked_to_rag_flows
          project_ids = flow_nodes_for_projects.map(&:project_id).compact.uniq

          if project_ids.empty?
            projects = []
          else
            projects_accessible_opts = accessible_options.merge(access_role: :all, selected_ids: project_ids)
            projects = Project.accessible_by_user(api_user_info.user, api_user_info.org, projects_accessible_opts)
          end
          resources = FlowNode.empty_flow
          resources.delete(:flow)
          # Here we load access_roles only for flows-related resources, which
          # improves rendering time by skipping individual get_access_roles() calls
          # for every resource, and improves tags loading time.
          accessible_options[:access_roles_only] = true
          resources_h = Hash.new
          [:data_sources, :data_sets, :data_sinks, :code_containers, :data_credentials, :data_credentials_groups].each do |m|
            resources_h[m] = api_user_info.user.send(m, api_user_info.org, accessible_options)
          end

          load_resources(origin_nodes + triggered_flows + triggering_flows + linked_to_rag_flows, resources)
        end

        children_map.transform_values!(&:uniq)

        flows = build_flows(api_user_info, origin_nodes, children_map, true)
        triggered_flows = build_flows(api_user_info, triggered_flows,  build_children_map(triggered_flows), false)
        triggering_flows = build_flows(api_user_info, triggering_flows, build_children_map(triggering_flows), false)

        {
          flows: flows,
          resources: resources,
          projects: projects,
          triggered_flows: triggered_flows,
          triggering_flows: triggering_flows,
          linked_flows: build_flows(api_user_info, @linked_flows, build_children_map(@linked_flows), false)
        }
      end

      def build_provisioning (flow_node)
        resources = Hash.new
        resources[:data_credentials] = Array.new
        resources[:code_containers] = Array.new
        resources[:data_sets] = Array.new

        res = flow_node.resource

        if res.is_a?(DataSink)
          resources[:data_sink] = res
          resources[:dependent_data_sources] = [res.data_source] if res.data_source.present?
          resources[:data_credentials] << res.data_credentials if res.data_credentials.present?
          res = res.data_set
        elsif !res.is_a?(DataSet)
          return resources
        end

        while res.is_a?(DataSet)
          resources[:data_sets] << res
          resources[:code_containers] << res.code_container if res.code_container.present?
          resources[:data_credentials] << res.data_credentials if res.data_credentials.present?
          res = (res.parent_data_set || res.data_source)
        end

        # Note, the order of rendering data_sets in the response
        # is significant: the infrastructure expects them to be
        # in top-down order in the array. We added them bottom-up,
        # so here we reverse the order.
        resources[:data_sets].reverse!
        resources[:data_source] = res
        resources[:data_credentials] << res.data_credentials if res&.data_credentials.present?
        resources[:origin_data_sinks] = []
        resources[:shared_data_sets] = []
        resources[:provisioning_flow] = true

        resources
     end

      private
      attr_accessor :api_user_info, :origin_nodes, :flows_only, :accessible_options, :sort_opts

      def build_children_map(origins)
        children_map = {}
        nodes = FlowNode.where(origin_node_id: origins.pluck(:id))
        nodes.each do |fn|
          children_map[fn.parent_node_id] ||= []
          children_map[fn.parent_node_id] << fn
        end
        children_map.transform_values!(&:uniq)
      end

      def get_triggered_flows(origin_nodes)
        origin_node_ids = origin_nodes.pluck(:origin_node_id)
        sink_node_ids = DataSink.where(origin_node_id: origin_node_ids).pluck(:flow_node_id)
        origin_ids = FlowTrigger.where(triggering_flow_node_id: sink_node_ids).pluck(:triggered_origin_node_id)
        filter_accessible(origin_ids)
      end

      def get_triggering_flows(origin_nodes)
        ids = FlowTrigger.where(triggered_origin_node_id: origin_nodes.pluck(:origin_node_id)).pluck(:triggering_flow_node_id)
        origin_ids = FlowNode.where(id: ids).pluck(:origin_node_id)
        filter_accessible(origin_ids)
      end

      def get_linked_flows(origin_nodes)
        origin_node_ids = origin_nodes.pluck(:origin_node_id)
        linked_flows_map = Hash.new

        FlowLink.where(left_origin_node_id: origin_node_ids)
          .or(FlowLink.where(right_origin_node_id: origin_node_ids))
          .where(link_type: FlowLinkType.types[:linked_flow])
          .pluck(:left_origin_node_id, :right_origin_node_id).each do |link|
            linked_flows_map[link[0]] ||= []
            linked_flows_map[link[1]] ||= []
            linked_flows_map[link[0]] << link[1]
            linked_flows_map[link[1]] << link[0]
          end

        # For rendering in the top-level "linked_flows" element,
        # we include only those linked origins that aren't already in
        # the input origin_nodes, which get rendered in "flows".
        linked_origin_node_ids = linked_flows_map.keys.select { |id| !origin_node_ids.include?(id) }

        filtered_linked_origin_nodes = filter_accessible(linked_origin_node_ids)
        rag_origin_node_ids = origin_nodes.select { |node| node.flow_type == FlowNode::Flow_Types[:rag] }.pluck(:id)
        linked_to_rag_origin_node_ids = linked_flows_map.values_at(*rag_origin_node_ids).flatten.compact.uniq
        linked_to_rag_origin_nodes = filtered_linked_origin_nodes.select { |node| linked_to_rag_origin_node_ids.include?(node.id) }
        [filtered_linked_origin_nodes, linked_flows_map, linked_to_rag_origin_nodes]
      end

      def filter_accessible(ids)
        return FlowNode.where(id: ids) if api_user_info.user.super_user?

        api_user_info.user.flow_nodes(api_user_info.org, access_role: :all, selected_ids: ids)
      end

      def build_flows(api_user_info, origin_nodes, children_map, show_triggers)
        if show_triggers
          origin_node_ids = origin_nodes.pluck(:origin_node_id)
          triggering_triggers = FlowTrigger.where(triggered_origin_node_id: origin_node_ids).preload(:triggering_flow_node)

          sink_node_ids = DataSink.where(origin_node_id: origin_node_ids).pluck(:flow_node_id)
          triggered_triggers = FlowTrigger.where(triggering_flow_node_id: sink_node_ids).preload(:triggered_origin_node, :triggering_flow_node)

          triggered_triggers_map = triggered_triggers.group_by(&:triggering_flow_node_id)
          triggering_triggers_map = triggering_triggers.group_by(&:triggered_origin_node_id)
        else
          triggered_triggers_map = nil
          triggering_triggers_map = nil
        end

        sort_by = sort_opts[:sort_by] || 'id'
        sort_order = sort_opts[:sort_order] || 'desc'
        sort_order = sort_order.downcase

        sort_by = 'last_run_id' if sort_by == 'run_id'
        sort_by = sort_by.to_sym

        origin_nodes = origin_nodes.sort do |a, b|
          a_val = a.try(sort_by)
          b_val = b.try(sort_by)

          if a_val.nil? && b_val.nil?
            0
          elsif a_val.nil?
            1
          elsif b_val.nil?
            -1
          else
            res = a_val <=> b_val
            res = -res if sort_order == 'desc'
            res
          end
        end

        origin_nodes.map do |fn|
          build_flow(api_user_info, fn, children_map, triggered_triggers_map, triggering_triggers_map)
        end
      end

      def build_flow(api_user_info, current_node, children_map, triggered_triggers_map = nil, triggering_triggers_map = nil)
        result = { node: current_node, children: [] }

        if triggered_triggers_map && triggered_triggers_map[current_node.id]
          result[:flow_triggers] ||= []
          result[:flow_triggers] += triggered_triggers_map[current_node.id]
        end

        if triggering_triggers_map && triggering_triggers_map[current_node.id]
          result[:flow_triggers] ||= []
          result[:flow_triggers] += triggering_triggers_map[current_node.id]
        end

        if @linked_flows_map&.key?(current_node.id)
          result[:linked_flows] = @linked_flows_map[current_node.id]
        end

        children = children_map[current_node.id]
        if children.present?

          children.each do |child|
            if current_node.can_traverse?(api_user_info, child)
              unless result[:children].map{|h| h[:node][:id] }.include?(child.id)
                result[:children] << build_flow(api_user_info, child, children_map, triggered_triggers_map)
              end
            end
          end

          if result[:children].present?
            result[:children].sort_by!{|a| [a[:node].shared_origin_node_id || 0, a[:node].id ] }
          end
        end

        result
      end

      def load_resources(origin_nodes, resources)
        return if origin_nodes.empty?

        origin_ids = origin_nodes.map(&:id)
        r_h = Hash.new
        if api_user_info.user.super_user? || api_user_info.org_admin?
          origin_nodes.group_by(&:org_id).each do |org_id, fns|
            cnd = { shared_origin_node_id: fns.map(&:origin_node_id) }

            # Note, Nexla-admin can traverse across org boundary,
            # but others cannot...
            cnd[:org_id] = org_id if !api_user_info.user.super_user?

            origin_ids += FlowNode.where(cnd).pluck(:origin_node_id).uniq
          end
        end

        r_h[:data_sources] = DataSource.where(origin_node_id: origin_ids).order('id desc').jit_preload
        r_h[:data_sets] = DataSet.where(origin_node_id: origin_ids).order('id desc').jit_preload
        r_h[:data_sinks] = DataSink.where(origin_node_id: origin_ids).order('id desc').jit_preload

        ids_for_credentials = Set.new
        ids_for_credentials_groups = Set.new

        r_h[:data_sources].each do |ds|
          ids_for_credentials << ds.data_credentials_id if ds.data_credentials_id
          ids_for_credentials_groups << ds.data_credentials_group_id if ds.data_credentials_group_id
        end

        r_h[:data_sinks].each do |ds|
          ids_for_credentials << ds.data_credentials_id if ds.data_credentials_id
          ids_for_credentials_groups << ds.data_credentials_group_id if ds.data_credentials_group_id
        end

        r_h[:data_sets].each do |ds|
          ids_for_credentials << ds.data_credentials_id if ds.data_credentials_id
        end

        r_h[:data_credentials] = DataCredentials.where(id: ids_for_credentials.to_a).order('id desc').jit_preload
        r_h[:data_credentials_groups] = DataCredentialsGroup.where(id: ids_for_credentials_groups.to_a).order('id desc').jit_preload

        ids_for_code_containers = r_h[:data_sets].map(&:code_container_id).compact.uniq
        r_h[:code_containers] = CodeContainer.where(id: ids_for_code_containers).order('id desc').jit_preload

        data_sets_query = FlowNode
                            .where(id: origin_nodes.map(&:id))
                            .where.not(shared_origin_node_id: nil)
                            .where.not(parent_node: nil)
                            .joins("INNER JOIN flow_nodes AS parent_node ON parent_node.id = flow_nodes.parent_node_id")
                            .joins("INNER JOIN data_sets ON parent_node.data_set_id = data_sets.id")
                            .where("data_sets.id IS NOT NULL")
                            .select("distinct parent_node.data_set_id").to_sql

        r_h[:shared_data_sets] = DataSet.where(Arel.sql("id IN (#{data_sets_query})")).order('id desc').jit_preload

        r_h.each do |k, v|
          if v.present?
            resources[k] += v.to_a
            resources[k].uniq!
          end
        end

      end
    end
  end
end
