/**
 * Header - Header component with logo, stats, and action buttons
 * Extracted from Browser.tsx for reusability
 */

import {
  Network,
  HardDrive,
  Clock,
  Activity,
  Zap,
  Loader2,
  MessageCircle,
  Shield,
  Database,
} from "lucide-react";
import { useNavigate } from "react-router-dom";
import type { DatabaseStats } from "../../utils/api";

interface HeaderProps {
  stats: DatabaseStats | null;
  connected: boolean;
  embedData: {
    stats: { running: boolean; processed: number; failed: number } | null;
    totalEmbeddings: number;
    pendingNodes: number;
    enabled: boolean;
  };
  embedTriggering: boolean;
  embedMessage: string | null;
  onRegenerateClick: () => void;
  onAIChatClick: () => void;
  onSecurityClick: () => void;
}

export function Header({
  stats,
  connected,
  embedData,
  embedTriggering,
  embedMessage,
  onRegenerateClick,
  onAIChatClick,
  onSecurityClick,
}: HeaderProps) {
  const navigate = useNavigate();
  const totalNodes = stats?.database?.nodes ?? 0;
  const pendingNodes = Math.max(0, embedData.pendingNodes ?? 0);
  const queueCompletePct =
    totalNodes > 0
      ? Math.max(
          0,
          Math.min(100, ((totalNodes - pendingNodes) / totalNodes) * 100),
        )
      : 100;

  const formatUptime = (seconds: number) => {
    const hours = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    return `${hours}h ${mins}m`;
  };

  return (
    <header className="bg-midnight-navy border-b border-midnight-graphite px-4 py-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          {/* copperDB Logo - Interwoven copper threads with verdigris nexus */}
          <svg
            viewBox="0 0 200 180"
            width="44"
            height="40"
            className="flex-shrink-0"
            role="img"
            aria-hidden="true"
          >
            {/* Three interwoven threads */}
            <path
              d="M 40 140 Q 30 100 50 70 Q 70 40 100 35 Q 130 30 145 55 Q 155 75 140 90"
              fill="none"
              stroke="#00B8C8"
              strokeWidth="12"
              strokeLinecap="round"
              opacity="0.9"
            />
            <path
              d="M 100 25 Q 100 50 85 75 Q 70 100 85 120 Q 100 140 100 165"
              fill="none"
              stroke="#00B8C8"
              strokeWidth="12"
              strokeLinecap="round"
            />
            <path
              d="M 160 140 Q 170 100 150 70 Q 130 40 100 35 Q 70 30 55 55 Q 45 75 60 90"
              fill="none"
              stroke="#00B8C8"
              strokeWidth="12"
              strokeLinecap="round"
              opacity="0.9"
            />
            {/* Central nexus */}
            <circle cx="100" cy="85" r="12" fill="#B87333" />
            <circle cx="100" cy="85" r="8" fill="#0C1622" />
            <circle cx="100" cy="85" r="5" fill="#B87333" />
            {/* Destiny nodes */}
            <circle cx="55" cy="65" r="5" fill="#B87333" opacity="0.8" />
            <circle cx="145" cy="65" r="5" fill="#B87333" opacity="0.8" />
            <circle cx="100" cy="140" r="5" fill="#B87333" opacity="0.8" />
            {/* Connecting lines */}
            <line
              x1="60"
              y1="67"
              x2="93"
              y2="82"
              stroke="#B87333"
              strokeWidth="1.5"
              opacity="0.4"
            />
            <line
              x1="140"
              y1="67"
              x2="107"
              y2="82"
              stroke="#B87333"
              strokeWidth="1.5"
              opacity="0.4"
            />
            <line
              x1="100"
              y1="135"
              x2="100"
              y2="92"
              stroke="#B87333"
              strokeWidth="1.5"
              opacity="0.4"
            />
          </svg>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-lg font-semibold text-text-primary">copperDB</h1>
              {stats?.server?.version ? (
                <span className="rounded-full border border-copper/30 bg-copper/10 px-2 py-0.5 text-[10px] font-medium uppercase tracking-[0.18em] text-copper">
                  v{stats.server.version}
                </span>
              ) : null}
            </div>
            <p className="text-xs text-silver-steel">
              The Graph Database That Learns
            </p>
          </div>
        </div>

        {/* Connection Status */}
        <div className="flex items-center gap-6">
          {stats?.database && (
            <>
              <div className="flex items-center gap-2 text-sm">
                <Network className="w-4 h-4 text-silver-steel" />
                <span className="text-silver-steel">
                  {stats.database.nodes?.toLocaleString() ?? "?"} nodes
                </span>
              </div>
              <div className="flex items-center gap-2 text-sm">
                <HardDrive className="w-4 h-4 text-silver-steel" />
                <span className="text-silver-steel">
                  {stats.database.edges?.toLocaleString() ?? "?"} edges
                </span>
              </div>
              <div className="flex items-center gap-2 text-sm">
                <Clock className="w-4 h-4 text-silver-steel" />
                <span className="text-silver-steel">
                  {formatUptime(stats.server?.uptime_seconds ?? 0)}
                </span>
              </div>
            </>
          )}
          <button
            type="button"
            onClick={onRegenerateClick}
            className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-all bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30"
            title="Warning: This will clear and regenerate ALL embeddings"
          >
            <Zap className="w-4 h-4" />
            <span>Regenerate all Embeddings</span>
          </button>
          {/* Embed Button */}
          <button
            type="button"
            disabled={embedTriggering}
            className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
              embedData.stats?.running
                ? "bg-amber-500/20 text-amber-400 border border-amber-500/30"
                : "bg-midnight-navy hover:bg-midnight-graphite text-silver-steel hover:text-text-primary border border-midnight-graphite"
            }`}
            title={`Total embeddings: ${embedData.totalEmbeddings}${
              embedData.stats
                ? `, Session: ${embedData.stats.processed} processed, ${embedData.stats.failed} failed`
                : ""
            }, Queue: ${queueCompletePct.toFixed(1)}% complete (${pendingNodes.toLocaleString()} pending / ${totalNodes.toLocaleString()} total nodes)`}
          >
            {embedTriggering || embedData.stats?.running ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Zap className="w-4 h-4" />
            )}
            <span>
              {embedData.stats?.running ? "Embedding..." : "Embeddings"}
            </span>
            <span className="text-xs text-copper">
              ({embedData.totalEmbeddings.toLocaleString()})
            </span>
            <span className="text-xs text-silver-steel">
              {queueCompletePct.toFixed(1)}% queue
            </span>
          </button>

          <button
            type="button"
            onClick={onAIChatClick}
            className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-all bg-copper/20 hover:bg-copper/30 text-copper border border-copper/30"
            title="Open AI Assistant"
          >
            <MessageCircle className="w-4 h-4" />
            <span>AI Assistant</span>
          </button>

          <button
            type="button"
            onClick={() => navigate("/databases")}
            className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-all bg-midnight-navy hover:bg-midnight-graphite text-silver-steel hover:text-text-primary border border-midnight-graphite"
            title="Manage Databases"
          >
            <Database className="w-4 h-4" />
            <span>Databases</span>
          </button>

          <button
            type="button"
            onClick={onSecurityClick}
            className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-all bg-midnight-navy hover:bg-midnight-graphite text-silver-steel hover:text-text-primary border border-midnight-graphite"
            title="Security & API Tokens"
          >
            <Shield className="w-4 h-4" />
            <span>Security</span>
          </button>

          <div
            className={`flex items-center gap-2 px-3 py-1 rounded-full ${
              connected
                ? "bg-verdigris/20 status-connected"
                : "bg-red-500/20"
            }`}
          >
            <Activity
              className={`w-4 h-4 ${
                connected ? "text-verdigris" : "text-red-400"
              }`}
            />
            <span
              className={`text-sm ${
                connected ? "text-verdigris" : "text-red-400"
              }`}
            >
              {connected ? "Connected" : "Disconnected"}
            </span>
          </div>
        </div>
      </div>
      {/* Embed Message Toast */}
      {embedMessage && (
        <div className="absolute top-16 right-4 bg-midnight-navy border border-midnight-graphite rounded-lg px-4 py-2 text-sm text-silver-steel shadow-lg">
          {embedMessage}
        </div>
      )}
    </header>
  );
}
