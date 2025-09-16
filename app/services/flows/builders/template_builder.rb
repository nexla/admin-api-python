module Flows
  module Builders
    class TemplateBuilder

      def initialize(api_user_info, flow_template)
        @api_user_info = api_user_info
        @flow_template = flow_template
        @template = flow_template.template
        @flow_type = nil
        @origin_node = nil
      end

      attr_reader :api_user_info, :flow_template, :template
      attr_accessor :origin_node, :flow_type

      def build
        flow_template.validate!
        template['flows'].each do |flow|
          @flow_type = flow['flow_type'] || FlowNode.default_flow_type
          ActiveRecord::Base.transaction do
            traverse_flow(flow)
          end
        end

        return origin_node
      end

      private

      def traverse_flow(node, parent = nil)
        resource = build_resource(node, parent)
        @origin_node = resource.origin_node if origin_node.nil?
        node['children'].each do |child|
          traverse_flow(child, resource)
        end
      end

      def build_resource(node, parent)
        node_id = node['id']
        if node_id.include?('data_source')
          build_data_source(node_id)
        elsif node_id.include?('data_set')
          build_data_set(node_id, parent)
        elsif node_id.include?('data_sink')
          build_data_sink(node_id, parent)
        end
      end

      def build_data_source(node_id)
        resource = template['data_sources'].find { |e| node_id == e['id'] }
        input = resource.except('id')
          .merge({
            flow_type: flow_type || FlowNode.default_flow_type
          })
        data_source = DataSource.build_from_input(api_user_info, input)
        data_source.activate! if [FlowNode::Flow_Types[:rag], FlowNode::Flow_Types[:api_server]].include?(flow_type)
        data_source
      end

      def build_data_set(node_id, parent)
        return unless parent

        resource = template['data_sets'].find { |e| node_id == e['id'] }
        if resource.key?('code_container_id')
          cc_resource = template['code_containers'].find { |cc| resource['code_container_id'] == cc['id'] }
          cc = CodeContainer.where(cc_resource.except('id')).first
          if cc.ai_function_type.present? && cc.custom_config.present?
            custom_config = { props: {} }
            if cc.custom_config.key?('props')
              cc.custom_config['props'].each do |prop|
                custom_config[:props].merge!(prop['name'] => prop['default'])
              end
            end
          end
          resource.delete('code_container_id')
        end

        resource['data_source_id'] = parent.id if parent.is_a?(DataSource)
        resource['parent_data_set_id'] = parent.id if parent.is_a?(DataSet)

        input = resource.except('id')
        input.merge!({status: DataSet::Statuses[:active]}) if flow_type == FlowNode::Flow_Types[:rag]
        input.merge!({code_container_id: cc.id}) if cc.present?
        input.merge!({custom_config: custom_config}) if custom_config.present?
        data_set = DataSet.build_from_input(api_user_info, input)
        data_set
      end

      def build_data_sink(node_id, parent)
        return unless parent

        resource = template['data_sinks'].find { |e| node_id == e['id'] }
        resource['data_set_id'] = parent.id if parent.is_a?(DataSet)

        input = resource.except('id')
        input.merge!({status: DataSink::Statuses[:active]}) if flow_type == FlowNode::Flow_Types[:rag]
        data_sink = DataSink.build_from_input(api_user_info, input)
        data_sink
      end
    end
  end
end
