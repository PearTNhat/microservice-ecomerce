"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { orderService } from "@/features/orders/services/order-service";
import { PaymentMethod } from "@/features/orders/types";
import { Product } from "@/features/products/types";
import { formatPrice } from "@/lib/utils";
import {
  AlertCircle,
  CheckCircle2,
  Clock,
  CreditCard,
  Flame,
  Loader2,
  PackageCheck,
  ShieldCheck,
  Sparkles,
  Truck,
  X,
  Zap,
} from "lucide-react";
import Image from "next/image";
import Link from "next/link";
import React, { useEffect, useState } from "react";

interface FlashSaleModalProps {
  product: Product | null;
  isOpen: boolean;
  onClose: () => void;
}

type ModalState = "FORM" | "QUEUED" | "SUCCESS" | "FAILED";

export const FlashSaleModal: React.FC<FlashSaleModalProps> = ({
  product,
  isOpen,
  onClose,
}) => {
  const [state, setState] = useState<ModalState>("FORM");
  const [orderToken, setOrderToken] = useState<string>("");
  const [createdOrderCode, setCreatedOrderCode] = useState<string>("");
  const [errorMessage, setErrorMessage] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [pollCount, setPollCount] = useState(0);

  // Form Fields
  const [formData, setFormData] = useState({
    customer_name: "",
    customer_email: "",
    customer_phone: "",
    shipping_address: "",
    payment_method: "COD" as PaymentMethod,
  });

  const [testMode, setTestMode] = useState<"SUCCESS" | "FAIL">("SUCCESS");

  // Pre-fill user data from localStorage
  useEffect(() => {
    if (isOpen) {
      setState("FORM");
      setErrorMessage("");
      setOrderToken("");
      setCreatedOrderCode("");
      setPollCount(0);

      const userStr = localStorage.getItem("user_info");
      if (userStr) {
        try {
          const u = JSON.parse(userStr);
          if (u) {
            setFormData((prev) => ({
              ...prev,
              customer_name: `${u.first_name || ""} ${u.last_name || ""}`.trim() || prev.customer_name,
              customer_email: u.email || prev.customer_email,
              customer_phone: u.phone || prev.customer_phone,
            }));
          }
        } catch (e) {}
      }
    }
  }, [isOpen, product]);

  // Polling effect when state == "QUEUED"
  useEffect(() => {
    if (state !== "QUEUED" || !orderToken) return;

    let timer: NodeJS.Timeout;
    let attempts = 0;
    const maxAttempts = 30; // 30 seconds max

    const checkStatus = async () => {
      try {
        attempts++;
        setPollCount(attempts);
        const res = await orderService.getFlashSaleStatus(orderToken);
        const statusData = res.data;

        if (statusData?.status === "SUCCESS") {
          setCreatedOrderCode(statusData.order_code || "");
          setState("SUCCESS");
          return;
        }

        if (statusData?.status === "FAILED") {
          setErrorMessage(statusData.reason || "Không thể tạo đơn hàng Flash Sale");
          setState("FAILED");
          return;
        }

        if (attempts >= maxAttempts) {
          setErrorMessage("Hệ thống xử lý đơn quá thời gian chờ, vui lòng kiểm tra lại trang Đơn mua");
          setState("FAILED");
          return;
        }

        // Retry polling in 1s
        timer = setTimeout(checkStatus, 1000);
      } catch (err: any) {
        if (attempts < maxAttempts) {
          timer = setTimeout(checkStatus, 1000);
        } else {
          setErrorMessage(err?.message || "Lỗi kiểm tra trạng thái đơn hàng");
          setState("FAILED");
        }
      }
    };

    // First check after 500ms
    timer = setTimeout(checkStatus, 500);

    return () => {
      if (timer) clearTimeout(timer);
    };
  }, [state, orderToken]);

  if (!isOpen || !product) return null;

  const discountPercent =
    product.discount_price && product.discount_price < product.price
      ? Math.round(((product.price - product.discount_price) / product.price) * 100)
      : 30;

  const currentPrice = product.discount_price || product.price;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    const token = localStorage.getItem("access_token");
    if (!token) {
      setErrorMessage("Vui lòng đăng nhập tài khoản trước khi tham gia Flash Sale");
      setState("FAILED");
      return;
    }

    if (
      !formData.customer_name ||
      !formData.customer_email ||
      !formData.customer_phone ||
      !formData.shipping_address
    ) {
      alert("Vui lòng điền đầy đủ thông tin giao hàng");
      return;
    }

    try {
      setLoading(true);
      setErrorMessage("");

      const customerName =
        testMode === "FAIL"
          ? `${formData.customer_name} [TEST_FAIL]`
          : formData.customer_name;

      const res = await orderService.createFlashSaleOrder({
        product_id: product.id,
        quantity: 1, // Flash Sale: Giới hạn 1 món
        customer_name: customerName,
        customer_email: formData.customer_email,
        customer_phone: formData.customer_phone,
        shipping_address: formData.shipping_address,
        payment_method: formData.payment_method,
      });

      if (res.data?.order_token) {
        setOrderToken(res.data.order_token);
        setState("QUEUED");
      } else {
        throw new Error(res.message || "Không thể tiếp nhận đơn Flash Sale");
      }
    } catch (err: any) {
      setErrorMessage(err?.message || "Sản phẩm Flash Sale đã hết hoặc bạn đã mua đợt này");
      setState("FAILED");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm animate-in fade-in duration-200">
      <div className="relative w-full max-w-lg bg-slate-900 border border-amber-500/30 rounded-3xl shadow-2xl overflow-hidden text-slate-100 flex flex-col max-h-[90vh]">
        {/* Header Header */}
        <div className="relative bg-gradient-to-r from-amber-600 via-red-600 to-rose-700 p-5 text-white flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <div className="p-2 bg-white/20 rounded-xl backdrop-blur-sm animate-pulse">
              <Flame className="w-5 h-5 text-yellow-300 fill-yellow-300" />
            </div>
            <div>
              <div className="flex items-center gap-1.5">
                <span className="text-xs font-black uppercase tracking-wider bg-black/30 px-2 py-0.5 rounded-full text-yellow-300 border border-yellow-300/30">
                  ⚡ GIỜ VÀNG GIÁ SỐC
                </span>
              </div>
              <h3 className="text-lg font-black tracking-tight mt-0.5">
                Săn Nhanh - Số Lượng Giới Hạn
              </h3>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-full hover:bg-white/20 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Product Snapshot Bar */}
        <div className="bg-slate-800/80 px-5 py-3 border-b border-slate-700/60 flex items-center gap-4">
          <div className="relative w-14 h-14 rounded-xl overflow-hidden bg-slate-700 shrink-0 border border-slate-600">
            <Image
              src={product.thumbnail || "/placeholder.png"}
              alt={product.name}
              fill
              sizes="56px"
              className="object-cover"
            />
          </div>
          <div className="flex-1 min-w-0">
            <h4 className="text-sm font-bold text-white truncate">
              {product.name}
            </h4>
            <div className="flex items-baseline gap-2 mt-0.5">
              <span className="text-base font-black text-rose-400">
                {formatPrice(currentPrice)}
              </span>
              {product.discount_price && product.discount_price < product.price && (
                <span className="text-xs text-slate-400 line-through">
                  {formatPrice(product.price)}
                </span>
              )}
              <span className="text-[10px] font-bold bg-rose-500/20 text-rose-300 border border-rose-500/30 px-1.5 py-0.5 rounded-md">
                -{discountPercent}%
              </span>
              <span className="text-[10px] font-bold bg-amber-500/20 text-amber-300 border border-amber-500/30 px-1.5 py-0.5 rounded-md ml-auto">
                Kho: {product.stock} chiếc
              </span>
            </div>
          </div>
        </div>

        {/* Dynamic Body Content */}
        <div className="p-6 overflow-y-auto space-y-4">
          {state === "FORM" && (
            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="p-3 bg-amber-500/10 border border-amber-500/20 rounded-xl text-xs text-amber-300 flex items-center gap-2">
                <ShieldCheck className="w-4 h-4 shrink-0 text-amber-400" />
                <span>Mỗi khách hàng chỉ được săn <strong>1 sản phẩm</strong> trong khung giờ này.</span>
              </div>

              <div className="space-y-3">
                <div>
                  <label className="text-xs font-semibold text-slate-300 mb-1 block">
                    Họ và tên người nhận *
                  </label>
                  <Input
                    required
                    placeholder="Nguyễn Văn A"
                    value={formData.customer_name}
                    onChange={(e) =>
                      setFormData({ ...formData, customer_name: e.target.value })
                    }
                    className="bg-slate-800 border-slate-700 text-white placeholder:text-slate-500 text-sm h-10"
                  />
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                  <div>
                    <label className="text-xs font-semibold text-slate-300 mb-1 block">
                      Số điện thoại *
                    </label>
                    <Input
                      required
                      placeholder="0912345678"
                      value={formData.customer_phone}
                      onChange={(e) =>
                        setFormData({ ...formData, customer_phone: e.target.value })
                      }
                      className="bg-slate-800 border-slate-700 text-white placeholder:text-slate-500 text-sm h-10"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-semibold text-slate-300 mb-1 block">
                      Email nhận hóa đơn *
                    </label>
                    <Input
                      required
                      type="email"
                      placeholder="email@example.com"
                      value={formData.customer_email}
                      onChange={(e) =>
                        setFormData({ ...formData, customer_email: e.target.value })
                      }
                      className="bg-slate-800 border-slate-700 text-white placeholder:text-slate-500 text-sm h-10"
                    />
                  </div>
                </div>

                <div>
                  <label className="text-xs font-semibold text-slate-300 mb-1 block">
                    Địa chỉ giao hàng chi tiết *
                  </label>
                  <Input
                    required
                    placeholder="Số nhà, Tên đường, Phường/Xã, Quận/Huyện, TP"
                    value={formData.shipping_address}
                    onChange={(e) =>
                      setFormData({
                        ...formData,
                        shipping_address: e.target.value,
                      })
                    }
                    className="bg-slate-800 border-slate-700 text-white placeholder:text-slate-500 text-sm h-10"
                  />
                </div>

                <div>
                  <label className="text-xs font-semibold text-slate-300 mb-1.5 block">
                    Phương thức thanh toán
                  </label>
                  <div className="grid grid-cols-2 gap-2">
                    {[
                      { id: "COD", label: "Tiền mặt (COD)", icon: Truck },
                      { id: "VNPAY", label: "VNPAY QR", icon: CreditCard },
                      { id: "MOMO", label: "Ví MoMo", icon: Zap },
                      { id: "BANK_TRANSFER", label: "Chuyển khoản", icon: PackageCheck },
                    ].map((item) => {
                      const Icon = item.icon;
                      const selected = formData.payment_method === item.id;
                      return (
                        <button
                          key={item.id}
                          type="button"
                          onClick={() =>
                            setFormData({
                              ...formData,
                              payment_method: item.id as PaymentMethod,
                            })
                          }
                          className={`flex items-center gap-2 p-2.5 rounded-xl border text-xs font-medium transition-all text-left ${
                            selected
                              ? "bg-rose-600/20 border-rose-500 text-rose-300 shadow-sm"
                              : "bg-slate-800/60 border-slate-700 text-slate-300 hover:border-slate-600"
                          }`}
                        >
                          <Icon className="w-4 h-4 text-rose-400 shrink-0" />
                          <span>{item.label}</span>
                        </button>
                      );
                    })}
                  </div>
                </div>
              </div>

              {/* Developer Test Scenario Selector */}
              <div className="p-3 rounded-2xl bg-slate-800/80 border border-slate-700/70 space-y-2.5 shadow-inner">
                <div className="flex items-center justify-between text-xs">
                  <span className="font-bold text-slate-200 flex items-center gap-1.5">
                    🧪 Kịch bản thử nghiệm:
                  </span>
                  <span className="text-[10px] bg-slate-700 text-slate-300 px-2 py-0.5 rounded-full font-mono">
                    Dev Mode
                  </span>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <button
                    type="button"
                    onClick={() => setTestMode("SUCCESS")}
                    className={`px-3 py-2 rounded-xl text-xs font-bold border transition-all flex items-center justify-center gap-1.5 ${
                      testMode === "SUCCESS"
                        ? "bg-emerald-500/20 border-emerald-500/80 text-emerald-300 shadow-md shadow-emerald-950"
                        : "bg-slate-900/60 border-slate-700/60 text-slate-400 hover:text-slate-200"
                    }`}
                  >
                    <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
                    🟢 Test Thành Công (OK)
                  </button>

                  <button
                    type="button"
                    onClick={() => setTestMode("FAIL")}
                    className={`px-3 py-2 rounded-xl text-xs font-bold border transition-all flex items-center justify-center gap-1.5 ${
                      testMode === "FAIL"
                        ? "bg-rose-500/20 border-rose-500/80 text-rose-300 shadow-md shadow-rose-950"
                        : "bg-slate-900/60 border-slate-700/60 text-slate-400 hover:text-slate-200"
                    }`}
                  >
                    <span className="w-2 h-2 rounded-full bg-rose-400 animate-ping" />
                    🔴 Test Thất Bại & Rollback
                  </button>
                </div>
                <p className="text-[11px] text-slate-400 leading-relaxed">
                  {testMode === "SUCCESS"
                    ? "✅ Trừ kho Redis ➔ Xếp hàng RabbitMQ ➔ Ghi DB PostgreSQL thành công ➔ Nhận mã đơn."
                    : "⚠️ Trừ kho Redis ➔ Vào RabbitMQ ➔ Worker mô phỏng lỗi DB ➔ Rollback hoàn trả kho & mở khóa User."}
                </p>
              </div>

              <div className="pt-2">
                <Button
                  type="submit"
                  disabled={loading}
                  size="lg"
                  className="w-full bg-gradient-to-r from-amber-500 via-red-500 to-rose-600 hover:from-amber-600 hover:to-rose-700 text-white font-black text-base shadow-lg shadow-rose-600/30 gap-2 h-12 rounded-xl"
                >
                  {loading ? (
                    <>
                      <Loader2 className="w-5 h-5 animate-spin" />
                      Đang xử lý khóa kho...
                    </>
                  ) : (
                    <>
                      <Zap className="w-5 h-5 fill-current" />
                      XÁC NHẬN SĂN HÀNG NGAY
                    </>
                  )}
                </Button>
              </div>
            </form>
          )}

          {/* QUEUED STATE: Live Polling */}
          {state === "QUEUED" && (
            <div className="py-8 text-center space-y-6">
              <div className="relative w-20 h-20 mx-auto">
                <div className="absolute inset-0 rounded-full bg-rose-500/20 animate-ping" />
                <div className="relative w-20 h-20 rounded-full bg-gradient-to-tr from-amber-500 to-rose-600 flex items-center justify-center shadow-lg shadow-rose-500/30">
                  <Loader2 className="w-10 h-10 text-white animate-spin" />
                </div>
              </div>

              <div className="space-y-2">
                <div className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full bg-amber-500/20 text-amber-300 text-xs font-bold border border-amber-500/30">
                  <Clock className="w-3.5 h-3.5" />
                  Đang xếp hàng tạo đơn trong RabbitMQ ({pollCount}s)
                </div>
                <h3 className="text-xl font-black text-white">
                  Đã khóa tồn kho thành công!
                </h3>
                <p className="text-xs text-slate-400 max-w-sm mx-auto">
                  Hệ thống đang điều phối đơn hàng ghi vào cơ sở dữ liệu an toàn. Vui lòng không đóng cửa sổ...
                </p>
              </div>

              <div className="w-full bg-slate-800 rounded-full h-2 overflow-hidden border border-slate-700">
                <div
                  className="bg-gradient-to-r from-amber-500 via-red-500 to-rose-500 h-full transition-all duration-300"
                  style={{ width: `${Math.min(100, pollCount * 10 + 20)}%` }}
                />
              </div>
            </div>
          )}

          {/* SUCCESS STATE */}
          {state === "SUCCESS" && (
            <div className="py-8 text-center space-y-6">
              <div className="w-20 h-20 rounded-full bg-emerald-500/20 border border-emerald-500/40 text-emerald-400 flex items-center justify-center mx-auto shadow-lg shadow-emerald-500/20 animate-in zoom-in-50">
                <CheckCircle2 className="w-12 h-12" />
              </div>

              <div className="space-y-2">
                <div className="inline-flex items-center gap-1 px-3 py-1 rounded-full bg-emerald-500/20 text-emerald-300 text-xs font-black border border-emerald-500/30">
                  <Sparkles className="w-3.5 h-3.5" />
                  SĂN DEAL THÀNH CÔNG!
                </div>
                <h3 className="text-2xl font-black text-white">
                  Đơn hàng của bạn đã hoàn tất
                </h3>
                <div className="inline-block bg-slate-800 px-4 py-2 rounded-xl border border-slate-700 text-sm font-mono text-emerald-400">
                  Mã đơn: <strong>{createdOrderCode || "ORD-FS-SUCCESS"}</strong>
                </div>
                <p className="text-xs text-slate-400">
                  Hóa đơn xác nhận và thông tin vận chuyển đã được gửi qua email <strong>{formData.customer_email}</strong>.
                </p>
              </div>

              <div className="flex gap-3 pt-2">
                <Link href="/orders" className="flex-1">
                  <Button
                    size="lg"
                    className="w-full bg-emerald-600 hover:bg-emerald-700 text-white font-bold h-11"
                  >
                    Xem Đơn Mua Của Bạn
                  </Button>
                </Link>
                <Button
                  variant="outline"
                  size="lg"
                  onClick={onClose}
                  className="border-slate-700 text-slate-300 hover:bg-slate-800 h-11"
                >
                  Tiếp Tục Mua Sắm
                </Button>
              </div>
            </div>
          )}

          {/* FAILED STATE */}
          {state === "FAILED" && (
            <div className="py-8 text-center space-y-6">
              <div className="w-20 h-20 rounded-full bg-rose-500/20 border border-rose-500/40 text-rose-400 flex items-center justify-center mx-auto shadow-lg shadow-rose-500/20">
                <AlertCircle className="w-12 h-12" />
              </div>

              <div className="space-y-2">
                <h3 className="text-xl font-black text-white">
                  Không thể hoàn tất đơn hàng
                </h3>
                <p className="text-sm text-rose-300 bg-rose-950/40 p-3 rounded-xl border border-rose-800/50">
                  {errorMessage}
                </p>
              </div>

              <div className="flex gap-3 pt-2">
                <Button
                  onClick={() => setState("FORM")}
                  className="flex-1 bg-slate-800 hover:bg-slate-700 text-white font-bold h-11"
                >
                  Thử Lại
                </Button>
                <Button
                  variant="outline"
                  onClick={onClose}
                  className="border-slate-700 text-slate-300 hover:bg-slate-800 h-11"
                >
                  Đóng
                </Button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
