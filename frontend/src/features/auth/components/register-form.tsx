"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Lock, Mail, Phone, User } from "lucide-react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import React, { useState } from "react";
import { authService } from "../services/auth-service";

export function RegisterForm() {
  const router = useRouter();
  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [email, setEmail] = useState("");
  const [phone, setPhone] = useState("");
  const [password, setPassword] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setError(null);

    try {
      await authService.register({
        first_name: firstName,
        last_name: lastName,
        email,
        phone,
        password,
      });

      // Đăng ký thành công -> Chuyển sang trang nhập OTP xác thực email
      router.push(`/verify-email?email=${encodeURIComponent(email)}`);
    } catch (err: any) {
      setError(err.message || "Đăng ký thất bại. Email có thể đã được sử dụng.");
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

      <div className="grid grid-cols-2 gap-3">
        <Input
          label="Họ"
          placeholder="Nguyễn"
          value={lastName}
          onChange={(e) => setLastName(e.target.value)}
          leftIcon={<User className="w-4 h-4" />}
          required
        />
        <Input
          label="Tên"
          placeholder="Văn A"
          value={firstName}
          onChange={(e) => setFirstName(e.target.value)}
          required
        />
      </div>

      <Input
        label="Địa chỉ Email (Nhận OTP)"
        type="email"
        placeholder="example@gmail.com"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        leftIcon={<Mail className="w-4 h-4" />}
        required
      />

      <Input
        label="Số điện thoại"
        type="tel"
        placeholder="0988xxxxxx"
        value={phone}
        onChange={(e) => setPhone(e.target.value)}
        leftIcon={<Phone className="w-4 h-4" />}
      />

      <Input
        label="Mật khẩu (Ít nhất 6 ký tự)"
        type="password"
        placeholder="••••••••"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        leftIcon={<Lock className="w-4 h-4" />}
        required
      />

      <Button type="submit" size="lg" className="w-full mt-2" isLoading={isLoading}>
        Tạo Tài Khoản
      </Button>

      <p className="text-center text-xs text-slate-500 pt-2">
        Đã có tài khoản?{" "}
        <Link href="/login" className="text-blue-600 font-bold hover:underline">
          Đăng nhập
        </Link>
      </p>
    </form>
  );
}
