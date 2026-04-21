// Bifrost: AI chat panel stub
// This component is a placeholder for the AI assistant integration.
import type { FC } from "react";

interface BifrostProps {
  isOpen: boolean;
  onClose: () => void;
}

export const Bifrost: FC<BifrostProps> = ({ isOpen, onClose }) => {
  if (!isOpen) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-end justify-end p-4 pointer-events-none">
      <div className="bg-gray-900 border border-gray-700 rounded-xl shadow-2xl w-96 max-h-[600px] flex flex-col pointer-events-auto">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <span className="text-sm font-semibold text-white">AI Assistant</span>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-white transition-colors"
            aria-label="Close"
          >
            ✕
          </button>
        </div>
        <div className="flex-1 flex items-center justify-center text-gray-500 text-sm p-4">
          AI assistant coming soon.
        </div>
      </div>
    </div>
  );
};
