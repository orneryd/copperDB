/**
 * RegenerateConfirmModal - Confirmation modal for regenerating embeddings
 * Extracted from Browser.tsx for reusability
 */

import { Zap } from "lucide-react";

interface RegenerateConfirmModalProps {
  isOpen: boolean;
  totalEmbeddings: number;
  onConfirm: () => void;
  onCancel: () => void;
}

export function RegenerateConfirmModal({
  isOpen,
  totalEmbeddings,
  onConfirm,
  onCancel,
}: RegenerateConfirmModalProps) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-norse-deep border border-midnight-graphite rounded-xl p-6 max-w-md mx-4 shadow-2xl">
        <div className="flex items-center gap-3 mb-4">
          <div className="p-2 bg-red-500/20 rounded-lg">
            <Zap className="w-6 h-6 text-red-400" />
          </div>
          <h3 className="text-lg font-semibold text-white">
            Regenerate All Embeddings?
          </h3>
        </div>
        <p className="text-silver-steel mb-2">
          This will{" "}
          <span className="text-red-400 font-medium">
            clear all existing embeddings
          </span>{" "}
          and regenerate them from scratch.
        </p>
        <p className="text-silver-steel text-sm mb-6">
          This operation runs in the background. You have{" "}
          <span className="text-copper">
            {totalEmbeddings.toLocaleString()}
          </span>{" "}
          embeddings that will be regenerated.
        </p>
        <div className="flex gap-3 justify-end">
          <button
            type="button"
            onClick={onCancel}
            className="px-4 py-2 rounded-lg text-silver-steel hover:text-white hover:bg-midnight-graphite transition-all"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={onConfirm}
            className="px-4 py-2 bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30 rounded-lg font-medium transition-all"
          >
            Yes, Regenerate All
          </button>
        </div>
      </div>
    </div>
  );
}

