"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Lock, Mail } from "lucide-react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import React, { useState } from "react";
import { authService } from "../services/auth-service";

export function LoginForm() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setError(null);

    try {
      const res = await authService.login({ email, password });
      if (res.status === "success" && res.data) {
        // Lưu token và thông tin user
        localStorage.setItem("access_token", res.data.token);
        localStorage.setItem("user_info", JSON.stringify(res.data.user));
        router.push("/");
        router.refresh();
      }
    } catch (err: any) {
      setError(err.message || "Đăng nhập thất bại. Vui lòng kiểm tra lại thông tin.");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      {error && (
        <div className="p-3 rounded-xl bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-800 text-red-600 dark:text-red-400 text-sm">
          {error}
        </div>
      )}

      <Input
        label="Địa chỉ Email"
        type="email"
        placeholder="example@gmail.com"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        leftIcon={<Mail className="w-4 h-4" />}
        required
      />

      <Input
        label="Mật khẩu"
        type="password"
        placeholder="••••••••"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        leftIcon={<Lock className="w-4 h-4" />}
        required
      />

      <div className="flex items-center justify-between text-xs">
        <label className="flex items-center gap-1.5 text-slate-500 cursor-pointer">
          <input type="checkbox" className="rounded text-blue-600 focus:ring-blue-500" />
          <span>Ghi nhớ đăng nhập</span>
        </label>
        <Link href="#" className="text-blue-600 hover:underline font-medium">
          Quên mật khẩu?
        </Link>
      </div>

      <Button type="submit" size="lg" className="w-full mt-2" isLoading={isLoading}>
        Đăng Nhập
      </Button>

      <p className="text-center text-xs text-slate-500 pt-2">
        Chưa có tài khoản?{" "}
        <Link href="/register" className="text-blue-600 font-bold hover:underline">
          Đăng ký ngay
        </Link>
      </p>
    </form>
  );
}
