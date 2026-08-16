import { LoginForm } from "@/features/auth/components/login-form";
import { Lock, Tv } from "lucide-react";
import Link from "next/link";

export const metadata = {
  title: "Đăng Nhập - ElectroHub",
  description: "Đăng nhập tài khoản khách hàng để mua sắm thiết bị điện máy.",
};

export default function LoginPage() {
  return (
    <div className="min-h-[80vh] flex items-center justify-center px-4 py-12">
      <div className="w-full max-w-md bg-white dark:bg-slate-900 rounded-3xl p-8 border border-slate-100 dark:border-slate-800 shadow-xl space-y-6">
        <div className="text-center space-y-2">
          <Link href="/" className="inline-flex items-center gap-2">
            <div className="w-10 h-10 rounded-xl bg-blue-600 flex items-center justify-center text-white">
              <Tv className="w-5 h-5" />
            </div>
            <span className="text-2xl font-black text-slate-900 dark:text-white">ElectroHub</span>
          </Link>
          <h1 className="text-xl font-bold text-slate-900 dark:text-white pt-2">
            Đăng Nhập Khách Hàng
          </h1>
          <p className="text-xs text-slate-500">
            Truy cập để quản lý đơn hàng và nhận ưu đãi độc quyền
          </p>
        </div>

        <LoginForm />
      </div>
    </div>
  );
}
