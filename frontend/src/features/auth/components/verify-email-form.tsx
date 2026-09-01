"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { KeyRound, Mail } from "lucide-react";
import { useRouter, useSearchParams } from "next/navigation";
import React, { useState } from "react";
import { authService } from "../services/auth-service";

export function VerifyEmailForm() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const initialEmail = searchParams.get("email") || "";

  const [email, setEmail] = useState(initialEmail);
  const [code, setCode] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setError(null);

    try {
      const res: any = await authService.verifyEmail({ email, code });
      if (res?.data?.token) {
        localStorage.setItem("access_token", res.data.token);
        if (res.data.user) {
          localStorage.setItem("user_info", JSON.stringify(res.data.user));
        }
        window.dispatchEvent(new Event("auth-changed"));
      }
      setSuccess(true);
      setTimeout(() => {
        router.push("/");
      }, 1500);
    } catch (err: any) {
      setError(err.message || "Mã OTP không chính xác hoặc đã hết hạn (15 phút).");
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

      {success && (
        <div className="p-3 rounded-xl bg-emerald-50 dark:bg-emerald-950/40 border border-emerald-200 dark:border-emerald-800 text-emerald-600 dark:text-emerald-400 text-sm font-semibold text-center">
          ✅ Xác thực thành công! Đang chuyển hướng sang Đăng nhập...
        </div>
      )}

      <Input
        label="Địa chỉ Email"
        type="email"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        leftIcon={<Mail className="w-4 h-4" />}
        required
      />

      <Input
        label="Mã OTP (6 chữ số trong Email)"
        type="text"
        placeholder="123456"
        value={code}
        onChange={(e) => setCode(e.target.value)}
        leftIcon={<KeyRound className="w-4 h-4" />}
        maxLength={6}
        required
      />

      <Button type="submit" size="lg" className="w-full mt-2" isLoading={isLoading}>
        Xác Nhận Kích Hoạt
      </Button>
    </form>
  );
}
