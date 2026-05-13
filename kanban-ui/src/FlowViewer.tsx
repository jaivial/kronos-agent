import { useCallback, useEffect, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { SDK_URL, type FlowSummary, type ReactFlowGraph } from "./config";

const STEP_ORDER = [
  "wide_research",
  "prompt_enhancer",
  "planner",
  "executor",
  "validator_1",
  "validator_2",
];

const STEP_COLORS: Record<string, string> = {
  received: "#6b7280",
  researching: "#8b5cf6",
  enhancing: "#f59e0b",
  planning: "#3b82f6",
  executing: "#10b981",
  validating_1: "#f97316",
  validating_2: "#ec4899",
  done: "#22c55e",
  failed: "#ef4444",
};

function StatusDot({ status }: { status: string }) {
  return (
    <span
      style={{
        display: "inline-block",
        width: 10,
        height: 10,
        borderRadius: "50%",
        backgroundColor: STEP_COLORS[status] || "#6b7280",
        marginRight: 8,
      }}
    />
  );
}

function PipelineBar({ currentStep, status }: { currentStep: string; status: string }) {
  const idx = STEP_ORDER.indexOf(currentStep);
  return (
    <div style={{ display: "flex", gap: 2, marginTop: 4 }}>
      {STEP_ORDER.map((s, i) => (
        <div
          key={s}
          title={s}
          style={{
            flex: 1,
            height: 4,
            borderRadius: 2,
            backgroundColor:
              status === "failed"
                ? i <= idx
                  ? "#ef4444"
                  : "#374151"
                : i < idx
                  ? "#22c55e"
                  : i === idx
                    ? STEP_COLORS[status] || "#3b82f6"
                    : "#374151",
          }}
        />
      ))}
    </div>
  );
}

export default function FlowViewer() {
  const [flows, setFlows] = useState<FlowSummary[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [nodes, setNodes] = useNodesState([]);
  const [edges, setEdges] = useEdgesState([]);
  const [loading, setLoading] = useState(false);

  const fetchFlows = useCallback(async () => {
    try {
      const res = await fetch(`${SDK_URL}/api/flows`);
      const data = await res.json();
      setFlows(data.flows || []);
    } catch {}
  }, []);

  useEffect(() => {
    fetchFlows();
    const iv = setInterval(fetchFlows, 5000);
    return () => clearInterval(iv);
  }, [fetchFlows]);

  const loadGraph = useCallback(
    async (flowId: string) => {
      setSelectedId(flowId);
      setLoading(true);
      try {
        const res = await fetch(`${SDK_URL}/api/flows/${flowId}/react-flow`);
        const graph: ReactFlowGraph = await res.json();
        setNodes(
          (graph.nodes || []).map((n: any) => ({
            ...n,
            type: n.type || "default",
            style: {
              background: "#1e293b",
              color: "#e2e8f0",
              border: `2px solid ${
                n.data?.impact > 0.8
                  ? "#ef4444"
                  : n.data?.impact > 0.5
                    ? "#f59e0b"
                    : "#3b82f6"
              }`,
              borderRadius: 8,
              padding: "8px 12px",
              fontSize: 12,
            },
          })),
        );
        setEdges(
          (graph.edges || []).map((e: any) => ({
            ...e,
            animated: true,
            style: { stroke: "#64748b", strokeWidth: 1.5 },
            labelStyle: { fill: "#94a3b8", fontSize: 10 },
          })),
        );
      } catch {
        setNodes([]);
        setEdges([]);
      }
      setLoading(false);
    },
    [setNodes, setEdges],
  );

  return (
    <div
      style={{
        display: "flex",
        height: "100vh",
        background: "#0f172a",
        color: "#e2e8f0",
        fontFamily: "system-ui, sans-serif",
      }}
    >
      {/* Sidebar */}
      <div
        style={{
          width: 340,
          borderRight: "1px solid #1e293b",
          overflow: "auto",
          padding: 16,
        }}
      >
        <h2 style={{ margin: 0, marginBottom: 16, fontSize: 16 }}>
          Pipeline Flows
        </h2>
        {flows.length === 0 && (
          <p style={{ color: "#64748b", fontSize: 13 }}>No flows yet</p>
        )}
        {flows.map((f) => (
          <div
            key={f.id}
            onClick={() => loadGraph(f.id)}
            style={{
              padding: "10px 12px",
              marginBottom: 8,
              borderRadius: 8,
              background:
                selectedId === f.id ? "#1e293b" : "transparent",
              border:
                selectedId === f.id
                  ? "1px solid #3b82f6"
                  : "1px solid transparent",
              cursor: "pointer",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", fontSize: 13 }}>
              <StatusDot status={f.status} />
              <span style={{ fontWeight: 600 }}>{f.id.slice(0, 8)}</span>
              <span
                style={{
                  marginLeft: "auto",
                  fontSize: 11,
                  color: "#64748b",
                }}
              >
                {f.status}
              </span>
            </div>
            <div
              style={{
                fontSize: 12,
                color: "#94a3b8",
                marginTop: 4,
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
            >
              {f.original_prompt?.slice(0, 60)}
            </div>
            <PipelineBar currentStep={f.current_step} status={f.status} />
          </div>
        ))}
      </div>

      {/* Main canvas */}
      <div style={{ flex: 1, position: "relative" }}>
        {loading && (
          <div
            style={{
              position: "absolute",
              top: 16,
              left: 16,
              zIndex: 10,
              background: "#1e293b",
              padding: "8px 16px",
              borderRadius: 8,
              fontSize: 13,
            }}
          >
            Loading graph...
          </div>
        )}
        {!selectedId && (
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              height: "100%",
              color: "#475569",
              fontSize: 14,
            }}
          >
            Select a flow to view its impact graph
          </div>
        )}
        {selectedId && (
          <ReactFlow
            nodes={nodes}
            edges={edges}
            fitView
            style={{ background: "#0f172a" }}
          >
            <Background color="#1e293b" gap={20} />
            <Controls
              style={{
                background: "#1e293b",
                borderRadius: 8,
                borderColor: "#334155",
              }}
            />
            <MiniMap
              nodeColor={() => "#3b82f6"}
              maskColor="rgba(15,23,42,0.8)"
              style={{ background: "#1e293b", borderColor: "#334155" }}
            />
          </ReactFlow>
        )}
      </div>
    </div>
  );
}
