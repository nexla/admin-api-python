module Access
  class RoleConvertor
    class << self
      def int_to_sym(role_int)
        return nil if role_int == nil
        AccessControls::ALL_ROLES_SET[role_int]
      end

      def sym_to_int(sym)
        AccessControls::ALL_ROLES_SET.index(sym.to_sym)
      end
    end
  end
end