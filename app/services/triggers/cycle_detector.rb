module Triggers
  class CycleDetector
    def initialize(origin_node)
      @origin_node = origin_node
      @graph = Hash.new {|hash, key| hash[key] = Set.new }
    end

    attr_reader :graph, :triggers_pool

    def detect
      @triggers_pool = load_triggers
      visited = Set.new

      # Build complete graph
      nodes_to_process = [@origin_node]
      processed_nodes = Set.new

      while (node = nodes_to_process.shift)
        next if processed_nodes.include?(node.id)
        processed_nodes.add(node.id)

        # Add parent nodes
        parents = find_parents(node)
        parents.each do |parent|
          graph[parent.id].add(NodeInfo.new(node.id, find_trigger(parent, node)))
          nodes_to_process << parent
        end

        # Add child nodes
        children = find_children(node)
        children.each do |child|
          graph[node.id].add(NodeInfo.new(child.id, find_trigger(node, child)))
          nodes_to_process << child
        end
      end

      # Find cycle in complete graph
      visited.clear
      recursion_stack = Set.new
      triggers_path = {}

      # Check all nodes for cycles
      graph.keys.each do |node_id|
        next if visited.include?(node_id)
        cycle = find_cycle(node_id, visited, recursion_stack, triggers_path)
        return cycle if cycle
      end

      nil
    end

    private

    def find_cycle(node_id, visited, recursion_stack, triggers_path)
      return nil unless graph.key?(node_id)

      visited.add(node_id)
      recursion_stack.add(node_id)

      graph[node_id].each do |neighbor_info|
        neighbor_id = neighbor_info.node_id
        trigger = neighbor_info.trigger

        if trigger.triggering_origin_node_id == trigger.triggered_origin_node_id
          # Self-trigger
          return [trigger]
        end

        triggers_path["#{node_id}-#{neighbor_id}"] = trigger

        if recursion_stack.include?(neighbor_id)
          cycle_info = reconstruct_cycle(neighbor_id, recursion_stack, triggers_path)
          return cycle_info
        end

        if !visited.include?(neighbor_id)
          cycle = find_cycle(neighbor_id, visited, recursion_stack, triggers_path)
          return cycle if cycle
        end
      end

      recursion_stack.delete(node_id)
      nil
    end

    def reconstruct_cycle(end_id, recursion_stack, path_triggers)
      stack_array = recursion_stack.to_a

      # Collect nodes in cycle starting from end_id
      cycle_nodes = []
      current_id = end_id

      loop do
        cycle_nodes << current_id

        # Find next node that has trigger to current
        next_id = stack_array.find do |id|
          path_triggers.key?("#{id}-#{current_id}")
        end

        current_id = next_id

        break unless current_id && current_id != end_id
      end

      # Reverse array to get correct order
      cycle_nodes.reverse!

      # Collect triggers between consecutive nodes
      cycle_triggers = []
      cycle_nodes.each_cons(2) do |from_id, to_id|
        trigger = path_triggers["#{from_id}-#{to_id}"]
        cycle_triggers << trigger if trigger
      end

      # Add closing trigger
      closing_trigger = path_triggers["#{cycle_nodes.last}-#{cycle_nodes.first}"]
      cycle_triggers << closing_trigger if closing_trigger

      cycle_triggers
    end

    def find_trigger(from_node, to_node)
      triggers_pool.find do |trigger|
        trigger.triggering_origin_node_id == from_node.id &&
          trigger.triggered_origin_node_id == to_node.id
      end
    end

    def load_triggers
      FlowTrigger.where(org_id: @origin_node.org_id).to_a
    end

    def find_parents(node)
      parent_triggers = triggers_pool.select do |trigger|
        trigger.triggered_origin_node_id == node.id
      end

      FlowNode.where(id: parent_triggers.map(&:triggering_origin_node_id)).to_a
    end

    def find_children(node)
      child_triggers = triggers_pool.select do |trigger|
        trigger.triggering_origin_node_id == node.id
      end

      FlowNode.where(id: child_triggers.map(&:triggered_origin_node_id)).to_a
    end
  end

  NodeInfo = Struct.new(:node_id, :trigger)
end