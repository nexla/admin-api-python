module RatingConcern
  extend ActiveSupport::Concern

  included do |base|
    def self.rating(name)
      ratings << name

      has_many :"#{name}_ratings", -> { where(rating_type: name) }, class_name: 'RatingVote', as: :item
    end

    def self.ratings
      @ratings ||= []
    end

    def ratings(name)
      raise ArgumentError.new("Unknown rating type #{name}") unless self.class.ratings.include?(name)

      {
        rating: send("#{name}_rating"),
        count: send("#{name}_rating_count")
      }
    end
  end
end
