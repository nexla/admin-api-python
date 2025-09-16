class FlowsReport

  def self.generate (org)
    raise Api::V1::ApiError.new(:bad_request, "Missing requried org") if !org.present?

    all_refs = Hash.new
    cnd = { org_id: org.id }

    References::Extractors::CodeContainers.extract_from('jolt_standard', all_refs, cnd)
    References::Extractors::CodeContainers.extract_from('jolt_custom', all_refs, cnd)

    report = Array.new
    FlowNode.where(cnd).order(id: :asc).where("id = origin_node_id").in_batches(of: 100) do |g|
      g.each do |fn|
        next if !fn.data_source.present?

        fr = Hash.new
        fr[:listing_time] = Time.now.to_s
        fr[:system] = "Nexla_production"
        fr[:org_id] = fn.org_id
        fr[:org_name] = fn.org&.name
        fr[:origin_node_id] = fn.id
        fr[:flow_name] = fn.name
        fr[:project_id] = fn.project_id
        fr[:project_name] = fn.project&.name
        fr[:data_source_id] = fn.data_source&.id
        fr[:data_source_name] = fn.data_source&.name
        fr[:data_source_created] = fn.data_source&.created_at&.to_s
        fr[:data_source_last_modified] = fn.data_source&.updated_at&.to_s
        fr[:data_source_status] = fn.data_source&.status

        sinks = DataSink.where(origin_node_id: fn.id, status: DataSource::Statuses[:active])
        fr[:destinations_active] = sinks.count
        fr[:destinations_active_since] = sinks.order(updated_at: :asc).first&.updated_at&.to_s
        fr[:destinations_active_most_recent] = sinks.order(updated_at: :desc).first&.updated_at&.to_s

        fr[:nexsets_last_modified] = DataSet.where(origin_node_id: fn.id)
          .order(updated_at: :desc).first&.updated_at&.to_s

        res = fn.resources
        non_ru = res[:code_containers].where(reusable: false).pluck(:id)
        ru = res[:code_containers].where(reusable: true).pluck(:id, :name)
        fr[:non_reusable_record_tx] = non_ru
        fr[:reusable_record_tx] = ru

        attr = Array.new
        non_ru.each { |id| attr = attr + all_refs[id] if all_refs[id].present? }
        ru.each { |id| attr = attr + all_refs[id] if all_refs[id].present? }
        fr[:reusable_attribute_tx] = attr.uniq

        report << fr
      end
    end
    report
  end

  def self.generate_by_destination (org, input_cnd = {})
    raise Api::V1::ApiError.new(:bad_request, "Missing requried org") if !org.present?

    all_refs = Hash.new
    cnd = { org_id: org.id }

    References::Extractors::CodeContainers.extract_from('jolt_standard', all_refs, cnd)
    References::Extractors::CodeContainers.extract_from('jolt_custom', all_refs, cnd)

    cnd = cnd.merge(input_cnd)
    report = Array.new

    FlowNode.where(cnd).order(id: :asc).where("id = origin_node_id").in_batches(of: 100) do |g|
      g.each do |fn|
        next if !fn.data_source.present?

        res = fn.resources
        ru = res[:code_containers].where(reusable: true).pluck(:id, :name)
        non_ru = res[:code_containers].where(reusable: false).pluck(:id)

        nexsets_last_modified =  DataSet.where(origin_node_id: fn.id)
          .order(updated_at: :desc).first&.updated_at&.to_s

        attr = Array.new
        non_ru.each { |id| attr = attr + all_refs[id] if all_refs[id].present? }
        ru.each { |id| attr = attr + all_refs[id] if all_refs[id].present? }
        has_reusable_transforms = ru.present?
        has_reusable_attribute_transforms = attr.uniq.present?

        DataSink.where(origin_node_id: fn.id, status: DataSource::Statuses[:active]).each do |sink|
          fr = Hash.new
          fr[:flow_name] = fn.name
          fr[:flow_id] = fn.id
          fr[:project_id] = fn.project_id
          fr[:project_name] = fn.project&.name

          fr[:data_source_name] = fn.data_source&.name
          fr[:data_source_id] = fn.data_source&.id
          fr[:data_source_conntector_type] = fn.data_source&.connector_type
          fr[:data_source_created] = fn.data_source&.created_at&.to_s
          fr[:data_source_last_modified] = fn.data_source&.updated_at&.to_s
          fr[:data_source_status] = fn.data_source&.status

          fr[:data_sink_name] = sink.name
          fr[:data_sink_id] = sink.id
          fr[:data_sink_conntector_type] = sink.connector_type
          fr[:data_sink_created] = sink.created_at&.to_s
          fr[:data_sink_last_modified] = sink.updated_at&.to_s
          fr[:data_sink_status] = sink.status
          fr[:has_reusable_transforms] = has_reusable_transforms
          fr[:has_reusable_attribute_transforms] = has_reusable_attribute_transforms
          fr[:nexsets_last_modified] = nexsets_last_modified

          ds = sink.data_set
          source_schema = {}
          while ds.present?
            if ds.source_schema.present?
              source_schema = ds.source_schema
              break
            end
            ds = ds.parent_data_set
          end

          fr[:source_schema] = source_schema.to_json

          report << fr
        end
      end
    end

    report
  end

end
