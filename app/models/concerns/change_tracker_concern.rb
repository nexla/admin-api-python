module ChangeTrackerConcern
  extend ActiveSupport::Concern

  included do
    attr_accessor :change_list
    before_update :accumulate_change_list
  end

  def accumulate_change_list
    self.change_list ||= {}
    new_changes = self.changes_to_save
    self.change_list.merge!(new_changes) do |_, old_val, new_val|
      [old_val[0], new_val[1]] # keep original before, update with latest after
    end
  end
end