import { VerifyEmailForm } from "@/features/auth/components/verify-email-form";
import { KeyRound, Tv } from "lucide-react";
import Link from "next/link";
import { Suspense } from "react";

export const metadata = {
  title: "Xác Thực Email OTP - ElectroHub",
  description: "Nhập mã OTP 6 số để kích hoạt tài khoản của bạn.",
};

export default function VerifyEmailPage() {
  return (
    <div className="min-h-[80vh] flex items-center justify-center px-4 py-12">
      <div className="w-full max-w-md bg-white dark:bg-slate-900 rounded-3xl p-8 border border-slate-100 dark:border-slate-800 shadow-xl space-y-6">
        <div className="text-center space-y-2">
          <div className="w-12 h-12 rounded-2xl bg-blue-500/10 text-blue-600 flex items-center justify-center mx-auto mb-2">
            <KeyRound className="w-6 h-6" />
          </div>
          <h1 className="text-xl font-bold text-slate-900 dark:text-white">
            Kích Hoạt Tài Khoản
          </h1>
          <p className="text-xs text-slate-500">
            Vui lòng nhập mã OTP 6 số đã được gửi tới hòm thư của bạn
          </p>
        </div>

        <Suspense fallback={<div className="text-center py-4 text-xs text-slate-400">Đang tải...</div>}>
          <VerifyEmailForm />
        </Suspense>
      </div>
    </div>
  );
}
