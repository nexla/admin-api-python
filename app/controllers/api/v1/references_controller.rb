module Api::V1
  class ReferencesController < Api::V1::ApiController
    def referenced_by
      model = params[:model].is_a?(String) ? params[:model].constantize : params[:model]
      item = model.find(params[:id])

      authorize! :read, item
      @referenced_by = ResourcesReference.where(referenced: item).map(&:referencing)
    end
  end
end