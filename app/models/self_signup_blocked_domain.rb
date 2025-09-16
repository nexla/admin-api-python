class SelfSignupBlockedDomain < ApplicationRecord
  validates_uniqueness_of :domain
end
