module Api::V1::DataSources
  class FlowTriggersController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      data_source = DataSource.find(params[:data_source_id])
      authorize! :read, data_source
      @flow_triggers = data_source.flow_triggers
      set_link_header(@flow_triggers)
    end

    def edit
      if params[:all].truthy?
        input = { flow_triggers: [] }
      else
        input = (validate_body_json FlowTriggersList).deep_symbolize_keys
      end

      @data_source = DataSource.find(params[:data_source_id])
      if (params[:mode] == :activate || params[:mode] == :pause)
        authorize! :operate, @data_source
      else
        authorize! :manage, @data_source
      end

      @data_source.update_flow_triggers(ApiUserInfo.new(current_user, current_org),
        input[:flow_triggers], params[:mode].try(:to_sym), params[:all].truthy?)
  
      @flow_triggers = @data_source.flow_triggers
      set_link_header(@flow_triggers)
      render "index"
    end
  end
end


