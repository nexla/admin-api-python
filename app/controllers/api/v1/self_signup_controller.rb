module Api
  module V1
    class SelfSignupController < Api::V1::ApiController
      RECAPTCHA_MINIMUM_SCORE = 0.5

      skip_before_action :authenticate, only: %i[create verify_email]

      before_action :authenticate_optional, only: [:create]
      before_action :require_self_signup_feature

      before_action :require_nexla_admin!, only: [:approve, :index]

      def create
        input = (validate_body_json SelfSignupRequest).symbolize_keys

        invite_uid = input.delete(:invite) || params[:invite]

        unless current_user
          verify_recaptcha!(input)
        end

        result = SelfSignupRequest.build_from_input(input, invite_uid, current_user, request.origin )
        PrometheusMetric.observe(:'sign_up_request_created')
        render json: result
      end

      def verify_email
        request = SelfSignupRequest.find_by(email_verification_token: params[:token])
        if request.nil?
          raise Api::V1::ApiError.new(:bad_request, 'Invalid verification token')
        end

        request.verify!

        if FeatureToggle.enabled?(:automatic_self_signup_approval)
          request.transaction do
            request.approve!
            request.create_user_and_org!
            request.destroy

            PrometheusMetric.observe(:'sign_up_auto_approved')
            PrometheusMetric.observe(:'sign_up_approved')
          end

          return render json: {result: :approved}
        end

        payload = {
          request_id: request.id,
          acquisition_type: request.invite ? 'invitation' : 'self-signup',
          email: request.email,
          full_name: request.full_name
        }
        UserEventsWebhooksWorker.perform_async('approval_pending', payload.as_json)
        render json: {result: :verified, message: "Your request will be reviewed by our team. You will receive an email once your request is approved."}
      end

      def approve
        request = SelfSignupRequest.find(params[:id])

        raise Api::V1::ApiError.new(:bad_request, 'Request already approved') if request.approved?
        raise Api::V1::ApiError.new(:bad_request, "Email has to be verified before approving") unless request.email_verified?

        request.transaction do
          request.approve!(current_user)
          request.create_user_and_org!
          request.destroy

          PrometheusMetric.observe(:'sign_up_manual_approved')
          PrometheusMetric.observe(:'sign_up_approved')
        end
        render json: {result: :ok}
      end

      def index
        status_filter = params[:status]
        requests = status_filter.present? ? SelfSignupRequest.where(status: status_filter.downcase) : SelfSignupRequest.all
        @requests = requests.page(@page).per_page(@per_page)
      end

      protected
      def require_self_signup_feature
        unless FeatureToggle.enabled?(:self_signup)
          raise Api::V1::ApiError.new(:method_not_allowed, 'Self signup is not available at the moment')
        end
      end

      def verify_recaptcha!(input)
        return if Rails.env.development? && ENV['SKIP_RECAPTCHA']

        token = input.delete(:g_captcha_response) || params[:g_captcha_response]
        if token.blank?
          raise Api::V1::ApiError.new(:bad_request, 'Recaptcha response required')
        end

        Recaptcha.verify_recaptcha!(token, nil, input[:email], request)
        PrometheusMetric.observe(:captcha_verified)
      end

    end
  end
end