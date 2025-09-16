class ResourcesReferencesOrigin < ApplicationRecord
  belongs_to :resources_reference, inverse_of: :resources_references_origins

  belongs_to :origin_node, class_name: 'FlowNode'
end