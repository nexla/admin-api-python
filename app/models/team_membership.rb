class TeamMembership < ApplicationRecord
  belongs_to :user
  belongs_to :team

  delegate :org, to: :team, allow_nil: true
end