if @dashboard_metrics
  json.status @status
  json.metrics @dashboard_metrics
end

json.partial! @api_root + "flows/show",
              flows: @flows,
              flows_only: @flows_only,
              resources: @resources,
              render_projects: @render_projects,
              triggered_flows: @triggered_flows,
              triggering_flows: @triggering_flows,
              linked_flows: @linked_flows
