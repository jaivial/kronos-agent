export const SDK_URL = import.meta.env.VITE_PIPELINE_SDK_URL || "http://localhost:8199";

export interface FlowSummary {
  id: string;
  status: string;
  current_step: string;
  original_prompt: string;
  project_path: string;
  created_at: string;
}

export interface ReactFlowGraph {
  nodes: any[];
  edges: any[];
}
