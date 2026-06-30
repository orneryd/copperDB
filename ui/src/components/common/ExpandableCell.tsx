/**
 * ExpandableCell - Expandable cell for table results
 * Extracted from Browser.tsx for reusability
 */

import { useState } from "react";
import { ChevronRight, ChevronDown } from "lucide-react";

interface ExpandableCellProps {
  data: unknown;
}

export function ExpandableCell({ data }: ExpandableCellProps) {
  const [expanded, setExpanded] = useState(false);

  if (data === null) return <span className="json-null">null</span>;
  if (data === undefined) return <span className="text-silver-structural">-</span>;

  // Primitive types - no expansion needed
  if (typeof data === "string") {
    const truncated = data.length > 50;
    return (
      <span className="json-string" title={truncated ? data : undefined}>
        {truncated ? data.slice(0, 50) + "..." : data}
      </span>
    );
  }
  if (typeof data === "number")
    return <span className="json-number">{data}</span>;
  if (typeof data === "boolean")
    return <span className="json-boolean">{String(data)}</span>;

  // Complex types - expandable
  const isArray = Array.isArray(data);
  const obj = data as Record<string, unknown>;

  // Try to extract node info for preview
  const id = obj._nodeId || obj.id || obj.elementId;
  const labels = obj.labels as string[] | undefined;
  const props =
    obj.properties && typeof obj.properties === "object"
      ? (obj.properties as Record<string, unknown>)
      : obj;
  const titleVal = props.title || props.name || props.text;
  const title = titleVal ? String(titleVal) : "";
  const type = obj.type;

  if (expanded) {
    return (
      <div className="relative">
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            setExpanded(false);
          }}
          className="absolute top-1 right-1 p-0.5 rounded bg-midnight-graphite hover:bg-silver-structural/30 z-10"
          title="Collapse"
        >
          <ChevronDown className="w-3 h-3 text-silver-steel" />
        </button>
        <pre className="text-xs text-silver-steel bg-midnight-navy/50 rounded p-2 pr-6 overflow-x-auto max-h-64 overflow-y-auto whitespace-pre-wrap">
          {JSON.stringify(data, null, 2)}
        </pre>
      </div>
    );
  }

  // Collapsed view - show preview
  return (
    <button
      type="button"
      onClick={(e) => {
        e.stopPropagation();
        setExpanded(true);
      }}
      className="flex items-center gap-1 hover:bg-midnight-graphite/50 rounded px-1 -mx-1 transition-colors text-left"
      title="Click to expand"
    >
      <ChevronRight className="w-3 h-3 text-silver-structural flex-shrink-0" />
      {id ? (
        <div className="flex items-center gap-1.5 min-w-0">
          {labels && labels.length > 0 ? (
            <span className="px-1 py-0.5 text-xs bg-verdigris/20 text-verdigris rounded flex-shrink-0">
              {labels[0]}
            </span>
          ) : typeof type === "string" ? (
            <span className="px-1 py-0.5 text-xs bg-verdigris/20 text-verdigris rounded flex-shrink-0">
              {type}
            </span>
          ) : null}
          <span className="text-copper text-xs truncate">
            {String(id).slice(0, 15)}
          </span>
          {title && (
            <span className="text-silver-steel truncate">
              {String(title).slice(0, 30)}
            </span>
          )}
        </div>
      ) : isArray ? (
        <span className="text-silver-steel">
          [{(data as unknown[]).length} items]
        </span>
      ) : (
        <span className="text-silver-steel">
          {"{"}...{Object.keys(obj).length} props{"}"}
        </span>
      )}
    </button>
  );
}

