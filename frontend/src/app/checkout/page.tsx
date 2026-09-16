"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useCartStore } from "@/features/cart/store/useCartStore";
import { flashSaleService } from "@/features/flash-sale/services/flash-sale-service";
import { orderService } from "@/features/orders/services/order-service";
import { formatPrice } from "@/lib/utils";
import {
  AlertCircle,
  ArrowLeft,
  CheckCircle2,
  CreditCard,
  Loader2,
  Lock,
  ShieldCheck,
  ShoppingBag,
  Truck,
  User,
  Zap,
} from "lucide-react";
import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  BasketQuoteResponse,
  PaymentMethod,
  PriceConflictItem,
  PriceConflictResponse,
  QuoteLineDTO,
} from "@/features/orders/types";

export default function CheckoutPage() {
  const router = useRouter();
  const {
    items,
    getTotalPrice,
    clearCart,
    syncFlashSaleOffers,
    hasFlashSaleItems,
  } = useCartStore();

  const [formData, setFormData] = useState({
    customer_name: "",
    customer_email: "",
    customer_phone: "",
    shipping_address: "",
    note: "",
  });

  const [paymentMethod, setPaymentMethod] = useState<PaymentMethod>("COD");
  const [isLoading, setIsLoading] = useState(false);
  const [isPolling, setIsPolling] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [quoteToken, setQuoteToken] = useState<string | undefined>(undefined);
  const [activeQuote, setActiveQuote] = useState<BasketQuoteResponse | null>(null);
  const [quoteStatus, setQuoteStatus] = useState<"idle" | "loading" | "ready" | "error">("idle");
  const [conflictInfo, setConflictInfo] = useState<{
    open: boolean;
    message: string;
    newQuoteToken?: string;
    newTotal?: number;
    affectedItems?: PriceConflictItem[];
    newItems?: QuoteLineDTO[];
  }>({
    open: false,
    message: "",
  });

  const totalPrice = getTotalPrice();
  const hasFlashSale = hasFlashSaleItems();

  // 19.1: Tạo fingerprint bất biến để chống re-render loop tại /checkout
  const basketFingerprint = useMemo(() => {
    return items
      .map((it) => `${it.product.id}:${it.quantity}`)
      .sort()
      .join(",");
  }, [items]);

  // 19.1: Monotonic request ID để huỷ kết quả bất đồng bộ quá hạn
  const currentRequestId = useRef(0);

  // 17.1, 18.2, 19.1: Lấy báo giá giỏ hàng phụ thuộc DUY NHẤT vào basketFingerprint
  useEffect(() => {
    const currentItems = useCartStore.getState().items;
    if (currentItems.length === 0) {
      setQuoteStatus("idle");
      setActiveQuote(null);
      return;
    }

    const reqId = ++currentRequestId.current;
    const ids = currentItems.map((it) => it.product.id);
    const quoteItems = currentItems.map((it) => ({
      product_id: it.product.id,
      quantity: it.quantity,
    }));

    setQuoteStatus("loading");

    orderService
      .getBasketQuote({ items: quoteItems })
      .then((res) => {
        if (currentRequestId.current !== reqId) return;
        if (res.data && res.data.quote_token) {
          setQuoteToken(res.data.quote_token);
          setActiveQuote(res.data);
          setQuoteStatus("ready");
        } else {
          setQuoteStatus("error");
          setErrorMsg("Không nhận được báo giá hợp lệ từ máy chủ.");
        }
      })
      .catch(() => {
        if (currentRequestId.current !== reqId) return;
        setQuoteStatus("error");
        setErrorMsg("Không thể tải báo giá sản phẩm. Vui lòng thử lại hoặc tải lại trang.");
      });

    flashSaleService
      .getBatchOffers(ids)
      .then((res) => {
        if (currentRequestId.current !== reqId) return;
        if (res.data && res.data.offers) {
          syncFlashSaleOffers(res.data.offers);
        }
      })
      .catch(() => {});
  }, [basketFingerprint, syncFlashSaleOffers]);

  const effectiveHasFlashSale = activeQuote
    ? activeQuote.items.some((i) => i.is_flash_sale)
    : hasFlashSale;

  // Bắt buộc chuyển sang COD nếu đơn hàng đang chứa Flash Sale
  useEffect(() => {
    if (effectiveHasFlashSale) {
      setPaymentMethod("COD");
    }
  }, [effectiveHasFlashSale]);

  // Tự động điền thông tin nếu đã đăng nhập
  useEffect(() => {
    const userStr = localStorage.getItem("user_info");
    if (userStr) {
      try {
        const user = JSON.parse(userStr);
        setFormData((prev) => ({
          ...prev,
          customer_name: `${user.first_name || ""} ${user.last_name || ""}`.trim() || user.email || "",
          customer_email: user.email || "",
        }));
      } catch (e) {}
    }
  }, []);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;
    setFormData((prev) => ({ ...prev, [name]: value }));
  };

  const handleCheckout = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrorMsg(null);

    // Kiểm tra đăng nhập
    const token = localStorage.getItem("access_token");
    if (!token) {
      setErrorMsg("Vui lòng đăng nhập tài khoản trước khi tiến hành đặt hàng.");
      return;
    }

    if (!formData.customer_name || !formData.customer_email || !formData.customer_phone || !formData.shipping_address) {
      setErrorMsg("Vui lòng điền đầy đủ các thông tin giao hàng bắt buộc (*).");
      return;
    }

    if (items.length === 0) {
      setErrorMsg("Giỏ hàng của bạn đang trống!");
      return;
    }

    if (hasFlashSale && paymentMethod !== "COD") {
      setErrorMsg("Đơn hàng có sản phẩm Flash Sale chỉ hỗ trợ hình thức thanh toán khi nhận hàng (COD).");
      return;
    }

    setIsLoading(true);

    try {
      // 1. Chuẩn bị danh sách sản phẩm để gửi
      const orderItems = items.map((it) => ({
        product_id: it.product.id,
        quantity: it.quantity,
      }));

      // 2. Tạo khóa Idempotency Key duy nhất (UUIDv4)
      const idempotencyKey = `ecom-order-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;

      // 3. Gửi request tạo đơn hàng sang Backend
      const res = await orderService.createOrder(
        {
          customer_name: formData.customer_name,
          customer_email: formData.customer_email,
          customer_phone: formData.customer_phone,
          shipping_address: formData.shipping_address,
          note: formData.note,
          payment_method: paymentMethod,
          from_cart: false,
          quote_token: quoteToken,
          items: orderItems,
        },
        idempotencyKey
      );

      if (res && res.data) {
        const orderId = res.data.id;

        // Nếu đơn hàng có Flash Sale và trạng thái ban đầu là PENDING (do 2-phase async Saga)
        if (res.data.order_status === "PENDING" && hasFlashSale) {
          setIsPolling(true);
          let finalStatus = "PENDING";
          for (let attempt = 0; attempt < 8; attempt++) {
            await new Promise((resolve) => setTimeout(resolve, 1000));
            try {
              const detail = await orderService.getOrderByID(orderId);
              if (detail.data && detail.data.order_status) {
                finalStatus = detail.data.order_status;
                if (finalStatus === "CONFIRMED") {
                  break;
                }
                if (finalStatus === "CANCELLED") {
                  setErrorMsg(
                    "Đơn hàng không thể hoàn tất do sản phẩm Flash Sale hoặc tồn kho thường đã hết suất ưu đãi!"
                  );
                  setIsLoading(false);
                  setIsPolling(false);
                  return;
                }
              }
            } catch (pollErr) {
              // Bỏ qua lỗi polling tạm thời và thử lại
            }
          }
        }

        // 4. Xóa giỏ hàng sau khi đặt thành công
        clearCart();
        // 5. Điều hướng sang trang chi tiết đơn hàng
        router.push(`/orders/${res.data.id}?created=true`);
      }
    } catch (err: any) {
      const isConflict =
        err.response?.status === 409 ||
        (err.message &&
          (err.message.includes("FLASH_SALE_OUT_OF_STOCK") ||
            err.message.includes("PRICE_CHANGED") ||
            err.message.includes("FLASH_SALE_QUOTA_EXCEEDED") ||
            err.message.includes("CONFLICT_REQUOTE_REQUIRED")));

      if (isConflict) {
        const conflictData: PriceConflictResponse | undefined =
          err.response?.data?.data || err.response?.data;
        const msg =
          conflictData?.message ||
          err.response?.data?.message ||
          err.message ||
          "Một số sản phẩm Flash Sale đã thay đổi giá hoặc hết suất ưu đãi.";

        setConflictInfo({
          open: true,
          message: msg,
          newQuoteToken: conflictData?.new_quote_token,
          newTotal: conflictData?.new_total,
          affectedItems: conflictData?.affected_items,
          newItems: conflictData?.new_items,
        });
        setErrorMsg(msg);
      } else {
        setErrorMsg(err.message || "Đặt hàng không thành công. Vui lòng thử lại!");
      }
    } finally {
      setIsLoading(false);
      setIsPolling(false);
    }
  };

  if (items.length === 0) {
    return (
      <div className="min-h-[70vh] flex flex-col items-center justify-center px-4 py-16 text-center">
        <div className="w-20 h-20 rounded-3xl bg-blue-50 dark:bg-slate-800 flex items-center justify-center text-blue-600 mb-6 shadow-sm">
          <ShoppingBag className="w-10 h-10" />
        </div>
        <h1 className="text-2xl font-bold text-slate-900 dark:text-white">
          Giỏ hàng của bạn đang trống
        </h1>
        <p className="text-slate-500 max-w-md mt-2 mb-8 text-sm">
          Bạn chưa có món hàng nào để thanh toán. Hãy tiếp tục dạo xem các sản phẩm điện máy đỉnh cao nhé!
        </p>
        <Link href="/products">
          <Button size="lg" className="shadow-lg shadow-blue-500/25 gap-2">
            <ArrowLeft className="w-4 h-4" /> Khám phá sản phẩm
          </Button>
        </Link>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-50/50 dark:bg-slate-950 py-10">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Modal Xung Đột Giá / Hết Suất Flash Sale (HTTP 409 Conflict Re-quote) */}
        {conflictInfo.open && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4 animate-in fade-in duration-200">
            <div className="bg-white dark:bg-slate-900 rounded-3xl p-6 sm:p-8 max-w-lg w-full shadow-2xl border border-amber-200 dark:border-amber-800 space-y-5">
              <div className="flex items-center gap-3 text-amber-600 dark:text-amber-400">
                <div className="w-12 h-12 rounded-2xl bg-amber-100 dark:bg-amber-950/80 flex items-center justify-center">
                  <Zap className="w-6 h-6" />
                </div>
                <div>
                  <h3 className="text-lg font-bold text-slate-900 dark:text-white">
                    Thông báo cập nhật ưu đãi Flash Sale
                  </h3>
                  <p className="text-xs text-slate-500">Mã lỗi: HTTP 409 Conflict (Re-quote)</p>
                </div>
              </div>

              <div className="p-4 rounded-2xl bg-amber-50 dark:bg-amber-950/40 border border-amber-200/60 dark:border-amber-900/60 text-sm text-slate-700 dark:text-slate-300">
                {conflictInfo.message}
              </div>

              {/* 18.4: Danh sách chi tiết sản phẩm bị biến động giá / hết suất sale */}
              {conflictInfo.affectedItems && conflictInfo.affectedItems.length > 0 && (
                <div className="space-y-2 border border-slate-200 dark:border-slate-800 rounded-2xl p-3 bg-slate-50/50 dark:bg-slate-950/50">
                  <p className="text-xs font-semibold text-slate-600 dark:text-slate-400">
                    Chi tiết sản phẩm thay đổi:
                  </p>
                  <div className="space-y-2 max-h-48 overflow-y-auto pr-1">
                    {conflictInfo.affectedItems.map((item, idx) => {
                      let reasonLabel = "Thay đổi giá";
                      if (item.reason === "FLASH_SALE_OUT_OF_STOCK") reasonLabel = "Hết suất sale";
                      else if (item.reason === "FLASH_SALE_EXPIRED") reasonLabel = "Hết hạn campaign";
                      else if (item.reason === "FLASH_SALE_QUOTA_EXCEEDED") reasonLabel = "Vượt giới hạn mua";

                      return (
                        <div
                          key={idx}
                          className="flex items-center justify-between text-xs p-2 rounded-xl bg-white dark:bg-slate-900 border border-slate-100 dark:border-slate-800"
                        >
                          <div className="flex-1 min-w-0 pr-2">
                            <p className="font-semibold text-slate-800 dark:text-slate-200 truncate">
                              {item.product_name}
                            </p>
                            <span className="inline-block mt-0.5 px-1.5 py-0.5 rounded text-[10px] font-medium bg-amber-100 dark:bg-amber-950 text-amber-700 dark:text-amber-300">
                              {reasonLabel}
                            </span>
                          </div>
                          <div className="text-right">
                            <span className="line-through text-slate-400 text-[11px] block">
                              {formatPrice(item.quoted_price)}
                            </span>
                            <span className="font-bold text-rose-600 dark:text-rose-400">
                              {formatPrice(item.updated_price)}
                            </span>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* 18.4: Hiển thị tổng tiền mới dự tính */}
              {conflictInfo.newTotal !== undefined && (
                <div className="p-3 bg-slate-100 dark:bg-slate-800/80 rounded-xl flex justify-between items-center text-sm font-semibold">
                  <span className="text-slate-700 dark:text-slate-300">Tổng tiền mới dự tính:</span>
                  <span className="text-base font-black text-rose-600 dark:text-rose-400">
                    {formatPrice(conflictInfo.newTotal)}
                  </span>
                </div>
              )}

              <p className="text-xs text-slate-500">
                Hệ thống không tự ý trừ tiền của bạn khi giá thay đổi. Bạn có thể chọn tiếp tục đặt hàng với giá hiện hành hoặc quay lại giỏ hàng.
              </p>

              <div className="flex flex-col sm:flex-row gap-3 pt-2">
                <Button
                  type="button"
                  variant="outline"
                  className="flex-1 rounded-xl"
                  onClick={() => router.push("/cart")}
                >
                  Quay lại giỏ hàng
                </Button>
                <Button
                  type="button"
                  className="flex-1 rounded-xl bg-amber-600 hover:bg-amber-700 text-white shadow-lg shadow-amber-600/25"
                  onClick={() => {
                    if (conflictInfo.newQuoteToken) {
                      setQuoteToken(conflictInfo.newQuoteToken);
                      if (conflictInfo.newItems && conflictInfo.newTotal !== undefined) {
                        setActiveQuote({
                          quote_token: conflictInfo.newQuoteToken,
                          total: conflictInfo.newTotal,
                          expires_at: Date.now() + 15 * 60 * 1000,
                          items: conflictInfo.newItems,
                        });
                      }
                      setQuoteStatus("ready");
                    }
                    syncFlashSaleOffers({});
                    setConflictInfo({ open: false, message: "" });
                    setErrorMsg(null);
                  }}
                >
                  Chấp nhận mua giá thường
                </Button>
              </div>
            </div>
          </div>
        )}

        {/* Header Breadcrumb */}
        <div className="mb-8">
          <Link
            href="/products"
            className="inline-flex items-center text-xs font-semibold text-slate-500 hover:text-blue-600 mb-3 transition gap-1.5"
          >
            <ArrowLeft className="w-3.5 h-3.5" /> Tiếp tục mua sắm
          </Link>
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <h1 className="text-2xl sm:text-3xl font-black text-slate-900 dark:text-white tracking-tight">
                Thanh Toán & Đặt Hàng
              </h1>
              <p className="text-sm text-slate-500 mt-1">
                Hoàn tất thông tin giao nhận để chúng tôi phục vụ bạn tốt nhất.
              </p>
            </div>
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-emerald-50 dark:bg-emerald-950/50 text-emerald-700 dark:text-emerald-400 text-xs font-semibold border border-emerald-200/50 dark:border-emerald-800/50">
              <ShieldCheck className="w-4 h-4" /> Bảo mật Idempotency 100%
            </div>
          </div>
        </div>

        {errorMsg && (
          <div className="mb-8 p-4 bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-800/50 rounded-2xl text-red-700 dark:text-red-300 text-sm flex items-start gap-3">
            <div className="w-2 h-2 rounded-full bg-red-500 mt-2 flex-shrink-0" />
            <div>
              <span className="font-bold">Lỗi xử lý đơn hàng:</span> {errorMsg}
              {errorMsg.includes("đăng nhập") && (
                <div className="mt-2">
                  <Link href="/login" className="underline font-semibold text-blue-600 dark:text-blue-400">
                    Bấm vào đây để đăng nhập ngay &rarr;
                  </Link>
                </div>
              )}
            </div>
          </div>
        )}

        <form onSubmit={handleCheckout} className="grid grid-cols-1 lg:grid-cols-12 gap-8">
          {/* CỘT TRÁI: FORM NHẬP THÔNG TIN (7 Cột) */}
          <div className="lg:col-span-7 space-y-6">
            {/* 1. Thông tin người nhận */}
            <div className="bg-white dark:bg-slate-900 rounded-3xl p-6 sm:p-8 shadow-sm border border-slate-100 dark:border-slate-800">
              <div className="flex items-center gap-3 mb-6 pb-4 border-b border-slate-100 dark:border-slate-800">
                <div className="w-8 h-8 rounded-xl bg-blue-100 dark:bg-blue-950 text-blue-600 flex items-center justify-center font-bold text-sm">
                  1
                </div>
                <div>
                  <h2 className="text-lg font-bold text-slate-900 dark:text-white flex items-center gap-2">
                    <User className="w-4 h-4 text-blue-600" /> Thông tin người nhận hàng
                  </h2>
                  <p className="text-xs text-slate-400">Địa chỉ và người nhận kiện hàng điện máy</p>
                </div>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-bold text-slate-700 dark:text-slate-300 mb-1.5">
                    Họ và tên người nhận <span className="text-red-500">*</span>
                  </label>
                  <Input
                    name="customer_name"
                    value={formData.customer_name}
                    onChange={handleChange}
                    placeholder="VD: Nguyễn Văn A"
                    required
                  />
                </div>

                <div>
                  <label className="block text-xs font-bold text-slate-700 dark:text-slate-300 mb-1.5">
                    Số điện thoại liên hệ <span className="text-red-500">*</span>
                  </label>
                  <Input
                    name="customer_phone"
                    value={formData.customer_phone}
                    onChange={handleChange}
                    placeholder="VD: 0912345678"
                    type="tel"
                    required
                  />
                </div>

                <div className="sm:col-span-2">
                  <label className="block text-xs font-bold text-slate-700 dark:text-slate-300 mb-1.5">
                    Địa chỉ Email nhận hóa đơn <span className="text-red-500">*</span>
                  </label>
                  <Input
                    name="customer_email"
                    value={formData.customer_email}
                    onChange={handleChange}
                    placeholder="VD: nguyenvana@gmail.com"
                    type="email"
                    required
                  />
                </div>

                <div className="sm:col-span-2">
                  <label className="block text-xs font-bold text-slate-700 dark:text-slate-300 mb-1.5">
                    Địa chỉ chi tiết nhận hàng (Số nhà, Đường, Phường, Quận/Huyện, Tỉnh) <span className="text-red-500">*</span>
                  </label>
                  <Input
                    name="shipping_address"
                    value={formData.shipping_address}
                    onChange={handleChange}
                    placeholder="VD: 123 Đường Điện Biên Phủ, Phường 15, Quận Bình Thạnh, TP.HCM"
                    required
                  />
                </div>

                <div className="sm:col-span-2">
                  <label className="block text-xs font-bold text-slate-700 dark:text-slate-300 mb-1.5">
                    Ghi chú đơn hàng (Tùy chọn)
                  </label>
                  <textarea
                    name="note"
                    value={formData.note}
                    onChange={handleChange}
                    placeholder="Giao giờ hành chính, gọi trước khi giao, lắp đặt tầng 2..."
                    rows={2}
                    className="w-full bg-slate-50 dark:bg-slate-800/60 border border-slate-200 dark:border-slate-700 rounded-xl p-3 text-sm text-slate-900 dark:text-white placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition"
                  />
                </div>
              </div>
            </div>

            {/* 2. Phương thức thanh toán */}
            <div className="bg-white dark:bg-slate-900 rounded-3xl p-6 sm:p-8 shadow-sm border border-slate-100 dark:border-slate-800">
              <div className="flex items-center gap-3 mb-6 pb-4 border-b border-slate-100 dark:border-slate-800">
                <div className="w-8 h-8 rounded-xl bg-blue-100 dark:bg-blue-950 text-blue-600 flex items-center justify-center font-bold text-sm">
                  2
                </div>
                <div>
                  <h2 className="text-lg font-bold text-slate-900 dark:text-white flex items-center gap-2">
                    <CreditCard className="w-4 h-4 text-blue-600" /> Phương thức thanh toán
                  </h2>
                  <p className="text-xs text-slate-400">Chọn hình thức thuận tiện nhất cho bạn</p>
                </div>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {/* Lựa chọn 1: COD */}
                <div
                  onClick={() => setPaymentMethod("COD")}
                  className={`cursor-pointer p-4 rounded-2xl border-2 transition-all flex items-start gap-3 ${
                    paymentMethod === "COD"
                      ? "border-blue-600 bg-blue-50/50 dark:bg-blue-950/30 text-blue-900 dark:text-blue-200"
                      : "border-slate-200 dark:border-slate-800 hover:border-slate-300 dark:hover:border-slate-700"
                  }`}
                >
                  <div className={`w-5 h-5 rounded-full border-2 flex items-center justify-center mt-0.5 ${
                    paymentMethod === "COD" ? "border-blue-600 bg-blue-600" : "border-slate-300"
                  }`}>
                    {paymentMethod === "COD" && <div className="w-2 h-2 rounded-full bg-white" />}
                  </div>
                  <div>
                    <span className="text-sm font-bold flex items-center gap-1.5">
                      Thanh toán khi nhận hàng (COD)
                      {hasFlashSale && (
                        <span className="text-[10px] bg-rose-600 text-white font-black px-1.5 py-0.5 rounded">
                          BẮT BUỘC CHO FLASH SALE
                        </span>
                      )}
                    </span>
                    <span className="text-xs text-slate-500 dark:text-slate-400 mt-0.5 block">
                      Kiểm tra hàng rồi thanh toán tiền mặt cho shipper
                    </span>
                  </div>
                </div>

                {/* Lựa chọn 2: VNPAY / QR */}
                <div
                  onClick={() => {
                    if (effectiveHasFlashSale) return;
                    setPaymentMethod("VNPAY");
                  }}
                  className={`p-4 rounded-2xl border-2 transition-all flex items-start gap-3 ${
                    effectiveHasFlashSale
                      ? "opacity-40 cursor-not-allowed border-slate-200 dark:border-slate-800 bg-slate-100/50 dark:bg-slate-800/30"
                      : paymentMethod === "VNPAY"
                      ? "cursor-pointer border-blue-600 bg-blue-50/50 dark:bg-blue-950/30 text-blue-900 dark:text-blue-200"
                      : "cursor-pointer border-slate-200 dark:border-slate-800 hover:border-slate-300 dark:hover:border-slate-700"
                  }`}
                >
                  <div className={`w-5 h-5 rounded-full border-2 flex items-center justify-center mt-0.5 ${
                    paymentMethod === "VNPAY" ? "border-blue-600 bg-blue-600" : "border-slate-300"
                  }`}>
                    {paymentMethod === "VNPAY" && <div className="w-2 h-2 rounded-full bg-white" />}
                  </div>
                  <div>
                    <span className="text-sm font-bold block">VNPAY / Quét mã QR</span>
                    <span className="text-xs text-slate-500 dark:text-slate-400 mt-0.5 block">
                      {effectiveHasFlashSale
                        ? "Tạm khóa do đơn hàng có Flash Sale (chỉ hỗ trợ COD)"
                        : "Thanh toán tức thì qua ứng dụng Ngân hàng"}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* CỘT PHẢI: TÓM TẮT ĐƠN HÀNG (5 Cột) */}
          <div className="lg:col-span-5">
            <div className="sticky top-24 bg-white dark:bg-slate-900 rounded-3xl p-6 sm:p-8 shadow-sm border border-slate-100 dark:border-slate-800 space-y-6">
              <div className="flex items-center justify-between pb-4 border-b border-slate-100 dark:border-slate-800">
                <h3 className="text-lg font-bold text-slate-900 dark:text-white">
                  Đơn hàng của bạn ({activeQuote?.items ? activeQuote.items.length : items.length} món)
                </h3>
                <Link href="/products" className="text-xs font-semibold text-blue-600 hover:underline">
                  Thay đổi
                </Link>
              </div>

              {/* 19.4: Render danh sách món hàng trực tiếp từ activeQuote máy chủ xác nhận */}
              <div className="max-h-72 overflow-y-auto space-y-3 pr-1">
                {activeQuote && activeQuote.items && activeQuote.items.length > 0
                  ? activeQuote.items.map((line) => {
                      const cartItem = items.find((it) => it.product.id === line.product_id);
                      const originalPrice = cartItem?.product.price || line.unit_price;
                      const thumbnail = cartItem?.product.thumbnail;

                      return (
                        <div key={line.product_id} className="flex gap-3 items-center">
                          <div className="relative w-14 h-14 rounded-xl overflow-hidden bg-slate-50 dark:bg-slate-800 border border-slate-200/60 dark:border-slate-700 flex-shrink-0">
                            {thumbnail ? (
                              <Image
                                src={thumbnail}
                                alt={line.product_name}
                                fill
                                className="object-cover"
                                sizes="56px"
                              />
                            ) : (
                              <div className="w-full h-full flex items-center justify-center text-[10px] text-slate-400">
                                Ảnh
                              </div>
                            )}
                            {line.is_flash_sale && (
                              <div className="absolute top-0.5 left-0.5 bg-rose-600 text-white text-[8px] font-black px-1 rounded">
                                SALE
                              </div>
                            )}
                          </div>
                          <div className="flex-1 min-w-0">
                            <p className="text-xs font-semibold text-slate-800 dark:text-slate-200 line-clamp-1">
                              {line.product_name}
                            </p>
                            <div className="flex items-center gap-1.5 text-[11px] text-slate-400 mt-0.5">
                              <span className={line.is_flash_sale ? "text-rose-600 font-bold" : ""}>
                                {formatPrice(line.unit_price)}
                              </span>
                              {line.is_flash_sale && originalPrice > line.unit_price && (
                                <span className="line-through text-[10px]">{formatPrice(originalPrice)}</span>
                              )}
                              <span>× {line.quantity}</span>
                            </div>
                          </div>
                          <div className="text-xs font-bold text-slate-900 dark:text-white text-right">
                            {formatPrice(line.subtotal)}
                          </div>
                        </div>
                      );
                    })
                  : items.map(({ product, quantity, isFlashSale, salePrice }) => {
                      const currentPrice =
                        isFlashSale && salePrice
                          ? salePrice
                          : product.discount_price || product.price;
                      const itemSubtotal = currentPrice * quantity;

                      return (
                        <div key={product.id} className="flex gap-3 items-center">
                          <div className="relative w-14 h-14 rounded-xl overflow-hidden bg-slate-50 dark:bg-slate-800 border border-slate-200/60 dark:border-slate-700 flex-shrink-0">
                            {product.thumbnail ? (
                              <Image
                                src={product.thumbnail}
                                alt={product.name}
                                fill
                                className="object-cover"
                                sizes="56px"
                              />
                            ) : (
                              <div className="w-full h-full flex items-center justify-center text-[10px] text-slate-400">
                                Ảnh
                              </div>
                            )}
                            {isFlashSale && (
                              <div className="absolute top-0.5 left-0.5 bg-rose-600 text-white text-[8px] font-black px-1 rounded">
                                SALE
                              </div>
                            )}
                          </div>
                          <div className="flex-1 min-w-0">
                            <p className="text-xs font-semibold text-slate-800 dark:text-slate-200 line-clamp-1">
                              {product.name}
                            </p>
                            <div className="flex items-center gap-1.5 text-[11px] text-slate-400 mt-0.5">
                              <span className={isFlashSale ? "text-rose-600 font-bold" : ""}>
                                {formatPrice(currentPrice)}
                              </span>
                              {isFlashSale && product.price > currentPrice && (
                                <span className="line-through text-[10px]">{formatPrice(product.price)}</span>
                              )}
                              <span>× {quantity}</span>
                            </div>
                          </div>
                          <div className="text-xs font-bold text-slate-900 dark:text-white text-right">
                            {formatPrice(itemSubtotal)}
                          </div>
                        </div>
                      );
                    })}
              </div>

              {/* Bảng tính tổng tiền */}
              {(() => {
                const finalPayableTotal = activeQuote ? activeQuote.total : totalPrice;
                return (
                  <div className="pt-4 border-t border-slate-100 dark:border-slate-800 space-y-2 text-sm">
                    <div className="flex justify-between text-slate-500">
                      <span>Tạm tính:</span>
                      <span className="font-semibold text-slate-800 dark:text-slate-200">
                        {formatPrice(finalPayableTotal)}
                      </span>
                    </div>
                    <div className="flex justify-between text-slate-500">
                      <span className="flex items-center gap-1">
                        <Truck className="w-3.5 h-3.5 text-emerald-600" /> Phí vận chuyển:
                      </span>
                      <span className="font-semibold text-emerald-600">Miễn phí toàn quốc</span>
                    </div>
                    <div className="pt-3 border-t border-slate-100 dark:border-slate-800 flex justify-between items-baseline">
                      <span className="text-base font-bold text-slate-900 dark:text-white">Tổng thanh toán:</span>
                      <div className="text-right">
                        <span className="text-2xl font-black text-blue-600 dark:text-blue-400 block">
                          {formatPrice(finalPayableTotal)}
                        </span>
                        <span className="text-[11px] text-slate-400">Đã bao gồm thuế VAT</span>
                      </div>
                    </div>
                  </div>
                );
              })()}

              {/* Nút Đặt Hàng */}
              <Button
                type="submit"
                size="lg"
                className={`w-full py-4 text-base font-bold tracking-wide shadow-xl ${
                  effectiveHasFlashSale
                    ? "bg-gradient-to-r from-rose-600 to-red-600 hover:from-rose-700 hover:to-red-700 shadow-rose-600/25"
                    : "shadow-blue-500/25"
                }`}
                disabled={isLoading || isPolling || quoteStatus !== "ready" || !quoteToken}
              >
                {quoteStatus === "loading" ? (
                  <span className="flex items-center gap-2">
                    <Loader2 className="w-5 h-5 animate-spin" />
                    Đang tải báo giá...
                  </span>
                ) : quoteStatus === "error" ? (
                  <span className="flex items-center gap-2">
                    <AlertCircle className="w-5 h-5" />
                    Lỗi báo giá - Vui lòng thử lại
                  </span>
                ) : isPolling ? (
                  <span className="flex items-center gap-2">
                    <Loader2 className="w-5 h-5 animate-spin" />
                    Đang xác nhận giữ chỗ Flash Sale...
                  </span>
                ) : (
                  <span className="flex items-center gap-2">
                    <Lock className="w-4 h-4" />
                    {effectiveHasFlashSale ? "⚡ XÁC NHẬN ĐƠN FLASH SALE (COD)" : "XÁC NHẬN ĐẶT HÀNG"}
                  </span>
                )}
              </Button>

              <div className="text-center text-[11px] text-slate-400 space-y-1">
                <p>⚡ Hệ thống trừ tồn kho an toàn tuyệt đối với Redis Lua Script</p>
                <p>🔒 Tự động chống gửi trùng đơn hàng qua Idempotency-Key</p>
              </div>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
}
