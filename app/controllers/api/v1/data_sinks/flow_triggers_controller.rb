module Api::V1::DataSinks
  class FlowTriggersController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      data_sink = DataSink.find(params[:data_sink_id])
      authorize! :read, data_sink
      @flow_triggers = data_sink.flow_triggers
      set_link_header(@flow_triggers)
    end

    def edit
      if params[:all].truthy?
        input = { flow_triggers: [] }
      else
        input = (validate_body_json FlowTriggersList).deep_symbolize_keys
      end

      @data_sink = DataSink.find(params[:data_sink_id])
      if (params[:mode] == :activate || params[:mode] == :pause)
        authorize! :operate, @data_sink
      else
        authorize! :manage, @data_sink
      end

      @data_sink.update_flow_triggers(ApiUserInfo.new(current_user, current_org),
        input[:flow_triggers], params[:mode].try(:to_sym), params[:all].truthy?)
  
      @flow_triggers = @data_sink.flow_triggers
      set_link_header(@flow_triggers)
      render "index"
    end
  end
end


