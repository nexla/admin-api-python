module Api::V1::DataSets
  class SharersController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      data_set = DataSet.find(params[:data_set_id])
      authorize! :read, data_set
      @sharers = data_set.sharers
      set_link_header(@sharers)
    end

    def edit
      input = (validate_body_json DataSetSharer).symbolize_keys

      @data_set = DataSet.find(params[:data_set_id])
      authorize! :manage, @data_set

      @data_set.update_sharers(input[:sharers], params[:mode], current_org)
      @sharers = @data_set.sharers
      set_link_header(@sharers)
      render "index"
    end

    def shared
      data_set = DataSet.find(params[:data_set_id])
      authorize! :manage, data_set

      users = User.where(:email => params[:email])
      user_ids = (users.collect(&:id))
      data_set_acls = DataSetsAccessControl.where(:data_set_id => params[:data_set_id],
        :accessor_id => user_ids, :accessor_type => 'USER')

      data_set_acls.each do |acl|
        acl.notified_at = Time.now
        acl.save!
      end

      external_sharers = ExternalSharer.where(:email => params[:email])

      external_sharers.each do |external|
        external.notified_at = Time.now
        external.save!
      end

      render :json => { :user_count => data_set_acls.length }, :status => :ok
    end
  end
end


