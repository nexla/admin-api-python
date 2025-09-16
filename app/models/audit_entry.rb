class AuditEntry

  Hidden_Attributes = [
    "credentials",
    "credentials_enc",
    "credentials_enc_iv"
  ]

  JSON_Attributes = [
    "source_config", "sink_config", "code_config", "code", "custom_config", "custom_config", "source_schema",
    "data_samples", "output_schema", "output_validation_schema", "source_path", "output_schema_annotations",
    "data_defaults", "map_entry_schema", "template_config", "config", "transform",
    "source_template", "sink_template", "schema", "annotations", "validations"
  ]

  def self.sort_by_date (entries, sort_order = :desc)
    entries.sort_by(&:created_at).tap do |sorted|
      sorted.reverse! if sort_order == :desc
    end
  end

  def initialize (org)
    @org = org.is_a?(Org) ? org : Org.find(org)
  end

  def log_for_resource (resource, date_interval, cnd = {}, negative_cnd = {})
    m = (resource.to_s.camelcase).constantize

    cnd = merge_conditions(cnd, { org_id: @org.id })
    m.audit_log(date_interval, cnd, negative_cnd).to_a
  end

  def all(date_interval, cnd = {}, negative_cnd ={}, sort_order = :desc)
    cnd = merge_conditions(cnd, { org_id: @org.id })
    entries = ConstantResolver.instance.versioned_models.map do |model_name|
      model_name.to_s.camelcase.constantize rescue nil # TODO: Remove rescue and do it in a better fashion
    end.compact.flat_map do |vm|
      vm.audit_log(date_interval, cnd, negative_cnd).to_a
    end

    AuditEntry.sort_by_date(entries, sort_order)
  end

  def merge_conditions(cnd, added_cnd)
    if cnd.is_a?(Hash)
      cnd = cnd.merge(added_cnd)
    else
      cnd = cnd.dup
      cnd << added_cnd
    end
    cnd
  end
end
