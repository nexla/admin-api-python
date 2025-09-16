class DataSets::Share < BaseAction
  association :data_set, ::DataSet
  association :user, ::User

  def call
    data_set.add_sharer(user, org)
  end
end
