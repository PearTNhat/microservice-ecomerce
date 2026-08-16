"use client";

import { Button } from "@/components/ui/button";
import { AlertCircle, RefreshCw } from "lucide-react";
import { useEffect } from "react";

export default function ErrorBoundary({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("Frontend Route Error:", error);
  }, [error]);

  return (
    <div className="min-h-[60vh] flex items-center justify-center px-4 py-16">
      <div className="max-w-md w-full text-center space-y-5 bg-white dark:bg-slate-900 p-8 rounded-3xl border border-slate-100 dark:border-slate-800 shadow-xl">
        <div className="w-14 h-14 rounded-2xl bg-rose-500/10 text-rose-600 flex items-center justify-center mx-auto">
          <AlertCircle className="w-7 h-7" />
        </div>
        <div className="space-y-2">
          <h2 className="text-xl font-bold text-slate-900 dark:text-white">
            Có lỗi xảy ra khi tải dữ liệu!
          </h2>
          <p className="text-xs text-slate-500">
            {error.message || "Vui lòng kiểm tra lại kết nối hoặc đảm bảo API Gateway & Backend đang hoạt động."}
          </p>
        </div>
        <Button onClick={() => reset()} variant="primary" className="gap-2 mx-auto">
          <RefreshCw className="w-4 h-4" /> Thử Lại
        </Button>
      </div>
    </div>
  );
}
