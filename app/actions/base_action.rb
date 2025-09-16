class BaseAction
  include ActiveModel::Model
  include ActiveModel::Attributes
  extend ActiveModel::Callbacks

  extend Memoist

  define_model_callbacks :validation, :perform, :commit

  class << self
    def associations
      @associations ||= (superclass.try(:associations) || {}).dup
    end

    def attribute(name, *rest, optional: false)
      super(name, *rest)
      validates name, presence: true unless optional
    end

    def association(name, klass)
      associations[name] = klass

      attribute(name)
    end
  end

  def initialize(arguments)
    args = arguments.with_indifferent_access
    self.class.associations.each do |name, klass|
      next if args[name] || args[name.to_s] || args["#{name}_id"].nil?

      arguments[name] = klass.find_by(id: args["#{name}_id"])
      arguments.delete("#{name}_id")
    end

    super(**arguments)
  end

  association :performer, ::User
  association :org, ::Org

  validates :performer, presence: true
  validates :org, presence: true

  def perform!
    run_callbacks :commit do
      ActiveRecord::Base.transaction do
        run_callbacks :validation do
          validate!
        end
        run_callbacks :perform do
          call
        end
      end
    end
  end

  def perform
    perform!
  rescue ActiveModel::ValidationError
    false
  end

  def call
    raise 'You should write your own #call method'
  end
end
