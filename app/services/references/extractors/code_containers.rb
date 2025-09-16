module References
  module Extractors

    class CodeContainers
      def self.extract_from(code_type, refs, cnd = {})
        cnd = { 
          resource_type: 'transform',
          code_type: code_type
        }.merge (cnd)

        cnd[:output_type] = 'record' if cnd[:output_type].blank?
        scope = CodeContainer.where(cnd)

        regexp = (code_type == "jolt_standard") ? /custom\s*\(\s*(\d+)/ :
          /nexla_fn\.call\s*\(\s*[\'\"]custom[\'\"]\s*\,\s*\[\s*(\d+)/

        all_ref_ids = Array.new

        scope.find_each do |cc|
          cc_refs = Array.new
          code = extract_code_string(cc.code_type, cc)
          if (code.blank?)
            next
          end
          code.scan(regexp).each do |match|
            id = match[0].to_i
            if numeric?(id)
              cc_refs << id
              all_ref_ids << id
            end
          end

          if !cc_refs.empty?
            refs[cc.id] = CodeContainer.where(id: cc_refs.uniq.sort).pluck(:id, :name)
          end
        end

        refs
      end

      def self.extract_code_string (code_type, cc)
        code = nil
        case code_type
        when "python", "javascript"
          code = cc.code
          code = Base64.decode64(code) if (cc.code_encoding == "base64")
        when "jolt_standard"
          code = cc.code.to_s
        when "jolt_custom"
          if cc.code.is_a?(Array)
            op = cc.code.find { |e| e["operation"] == "nexla.custom" }
            if op.present?
              code = op.dig("spec", "script")
              enc = op.dig("spec", "encoding")
              code = Base64.decode64(code) if !code.blank? && !enc.blank? && (enc.downcase == "base64")
            end
          end
        end
        code
      end

      def self.numeric?(val)
        true if Float(val) rescue false
      end
    end
  end
end
