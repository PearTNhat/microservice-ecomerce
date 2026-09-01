"use client";

import { Button } from "@/components/ui/button";
import { orderService } from "@/features/orders/services/order-service";
import { Order, OrderStatus } from "@/features/orders/types";
import { formatPrice } from "@/lib/utils";
import {
  AlertCircle,
  ArrowLeft,
  Calendar,
  CheckCircle2,
  Clock,
  CreditCard,
  Mail,
  MapPin,
  Package,
  Phone,
  ShieldCheck,
  Truck,
  User,
} from "lucide-react";
import Image from "next/image";
import Link from "next/link";
import { useParams, useSearchParams } from "next/navigation";
import { useEffect, useState } from "react";

function getStatusBadge(status: OrderStatus) {
  switch (status) {
    case "PENDING":
      return <span className="px-3 py-1 text-xs font-bold rounded-full bg-amber-50 text-amber-700 border border-amber-200 dark:bg-amber-950/50 dark:text-amber-300 dark:border-amber-800/50">Đang chờ xử lý</span>;
    case "CONFIRMED":
      return <span className="px-3 py-1 text-xs font-bold rounded-full bg-blue-50 text-blue-700 border border-blue-200 dark:bg-blue-950/50 dark:text-blue-300 dark:border-blue-800/50">Đã xác nhận</span>;
    case "PROCESSING":
      return <span className="px-3 py-1 text-xs font-bold rounded-full bg-purple-50 text-purple-700 border border-purple-200 dark:bg-purple-950/50 dark:text-purple-300 dark:border-purple-800/50">Đang đóng gói</span>;
    case "SHIPPING":
      return <span className="px-3 py-1 text-xs font-bold rounded-full bg-indigo-50 text-indigo-700 border border-indigo-200 dark:bg-indigo-950/50 dark:text-indigo-300 dark:border-indigo-800/50">Đang vận chuyển</span>;
    case "COMPLETED":
      return <span className="px-3 py-1 text-xs font-bold rounded-full bg-emerald-50 text-emerald-700 border border-emerald-200 dark:bg-emerald-950/50 dark:text-emerald-300 dark:border-emerald-800/50">Hoàn thành</span>;
    case "CANCELLED":
      return <span className="px-3 py-1 text-xs font-bold rounded-full bg-red-50 text-red-700 border border-red-200 dark:bg-red-950/50 dark:text-red-300 dark:border-red-800/50">Đã hủy</span>;
    default:
      return <span className="px-3 py-1 text-xs font-bold rounded-full bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300">{status}</span>;
  }
}

export default function OrderDetailPage() {
  const params = useParams();
  const searchParams = useSearchParams();
  const isJustCreated = searchParams.get("created") === "true";

  const orderID = Number(params.id);

  const [order, setOrder] = useState<Order | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchOrderDetail() {
      if (!orderID) return;

      try {
        setIsLoading(true);
        setError(null);
        const res = await orderService.getOrderByID(orderID);
        if (res && res.data) {
          setOrder(res.data);
        }
      } catch (err: any) {
        setError(err.message || "Không thể tải thông tin đơn hàng");
      } finally {
        setIsLoading(false);
      }
    }

    fetchOrderDetail();
  }, [orderID]);

  if (isLoading) {
    return (
      <div className="min-h-[60vh] flex items-center justify-center">
        <div className="animate-spin rounded-full h-10 w-10 border-4 border-blue-600 border-t-transparent" />
      </div>
    );
  }

  if (error || !order) {
    return (
      <div className="min-h-[60vh] flex flex-col items-center justify-center p-6 text-center">
        <AlertCircle className="w-12 h-12 text-red-500 mb-3" />
        <h2 className="text-xl font-bold text-slate-900 dark:text-white">Không tìm thấy đơn hàng</h2>
        <p className="text-sm text-slate-500 mt-1 mb-6">{error || "Đơn hàng này không tồn tại hoặc bạn không có quyền truy cập."}</p>
        <Link href="/orders">
          <Button variant="outline">Xem tất cả đơn hàng</Button>
        </Link>
      </div>
    );
  }

  const formattedDate = new Date(order.created_at).toLocaleDateString("vi-VN", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });

  return (
    <div className="min-h-screen bg-slate-50/50 dark:bg-slate-950 py-10">
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 space-y-6">
        {/* Just Created Banner */}
        {isJustCreated && (
          <div className="p-6 bg-gradient-to-r from-emerald-500 to-teal-600 rounded-3xl text-white shadow-lg shadow-emerald-500/20 flex items-center gap-4">
            <div className="w-12 h-12 rounded-2xl bg-white/20 flex items-center justify-center flex-shrink-0">
              <CheckCircle2 className="w-7 h-7 text-white" />
            </div>
            <div>
              <h2 className="text-lg font-black">🎉 ĐẶT HÀNG THÀNH CÔNG!</h2>
              <p className="text-xs text-emerald-50 mt-0.5">
                Cảm ơn bạn đã tin tưởng mua sắm thiết bị điện máy tại ElectroHub. Email xác nhận đơn hàng đang được gửi đến hộp thư của bạn.
              </p>
            </div>
          </div>
        )}

        {/* Navigation Breadcrumb */}
        <div className="flex items-center justify-between">
          <Link
            href="/orders"
            className="inline-flex items-center text-xs font-semibold text-slate-500 hover:text-blue-600 transition gap-1.5"
          >
            <ArrowLeft className="w-3.5 h-3.5" /> Danh sách đơn hàng
          </Link>
          <div className="flex items-center gap-2 text-xs text-slate-400">
            <Calendar className="w-3.5 h-3.5" /> Ngày đặt: {formattedDate}
          </div>
        </div>

        {/* Main Order Card */}
        <div className="bg-white dark:bg-slate-900 rounded-3xl p-6 sm:p-8 shadow-sm border border-slate-100 dark:border-slate-800 space-y-6">
          {/* Header */}
          <div className="flex flex-wrap items-center justify-between gap-4 pb-6 border-b border-slate-100 dark:border-slate-800">
            <div>
              <span className="text-xs text-slate-400 font-semibold uppercase tracking-wider block">
                Mã đơn hàng
              </span>
              <h1 className="text-xl sm:text-2xl font-black font-mono text-blue-600 dark:text-blue-400 mt-0.5">
                {order.order_code}
              </h1>
            </div>
            <div className="flex items-center gap-3">
              {getStatusBadge(order.order_status)}
            </div>
          </div>

          {/* Delivery & Recipient Info Grid */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-6 p-4 rounded-2xl bg-slate-50 dark:bg-slate-800/50 text-sm">
            <div className="space-y-2">
              <h3 className="text-xs font-bold uppercase text-slate-400 tracking-wider flex items-center gap-1.5">
                <User className="w-3.5 h-3.5 text-blue-600" /> Thông tin người nhận
              </h3>
              <p className="font-bold text-slate-900 dark:text-white">{order.customer_name}</p>
              <p className="text-xs text-slate-600 dark:text-slate-400 flex items-center gap-1.5">
                <Phone className="w-3.5 h-3.5" /> {order.customer_phone}
              </p>
              <p className="text-xs text-slate-600 dark:text-slate-400 flex items-center gap-1.5">
                <Mail className="w-3.5 h-3.5" /> {order.customer_email}
              </p>
            </div>

            <div className="space-y-2">
              <h3 className="text-xs font-bold uppercase text-slate-400 tracking-wider flex items-center gap-1.5">
                <MapPin className="w-3.5 h-3.5 text-blue-600" /> Địa chỉ giao nhận
              </h3>
              <p className="text-xs text-slate-700 dark:text-slate-300 leading-relaxed font-medium">
                {order.shipping_address}
              </p>
              {order.note && (
                <p className="text-xs text-slate-500 italic mt-1">
                  Ghi chú: {order.note}
                </p>
              )}
            </div>
          </div>

          {/* Items Table */}
          <div>
            <h3 className="text-sm font-bold text-slate-900 dark:text-white mb-4 flex items-center gap-2">
              <Package className="w-4 h-4 text-blue-600" /> Chi tiết các sản phẩm ({order.items?.length || 0})
            </h3>
            <div className="divide-y divide-slate-100 dark:divide-slate-800">
              {order.items?.map((item, idx) => (
                <div key={idx} className="py-3.5 flex gap-4 items-center">
                  <div className="relative w-16 h-16 rounded-xl overflow-hidden bg-slate-50 dark:bg-slate-800 border border-slate-200/60 dark:border-slate-700 flex-shrink-0">
                    {item.thumbnail ? (
                      <Image
                        src={item.thumbnail}
                        alt={item.product_name}
                        fill
                        className="object-cover"
                        sizes="64px"
                      />
                    ) : (
                      <div className="w-full h-full flex items-center justify-center text-xs text-slate-400">
                        Ảnh
                      </div>
                    )}
                  </div>

                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-semibold text-slate-900 dark:text-white line-clamp-1">
                      {item.product_name}
                    </p>
                    <p className="text-xs text-slate-400 mt-1">
                      Đơn giá: {formatPrice(item.price)} × {item.quantity}
                    </p>
                  </div>

                  <div className="text-sm font-bold text-slate-900 dark:text-white text-right">
                    {formatPrice(item.subtotal)}
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Payment & Summary */}
          <div className="pt-4 border-t border-slate-100 dark:border-slate-800 space-y-3">
            <div className="flex justify-between text-xs text-slate-500">
              <span>Hình thức thanh toán:</span>
              <span className="font-semibold text-slate-800 dark:text-slate-200">
                {order.payment_method} ({order.payment_status})
              </span>
            </div>
            <div className="flex justify-between text-xs text-slate-500">
              <span>Phí vận chuyển:</span>
              <span className="font-semibold text-emerald-600">Miễn phí toàn quốc</span>
            </div>
            <div className="flex justify-between items-baseline pt-2 border-t border-slate-100 dark:border-slate-800">
              <span className="text-base font-bold text-slate-900 dark:text-white">Tổng cộng:</span>
              <span className="text-2xl font-black text-blue-600 dark:text-blue-400">
                {formatPrice(order.total_amount)}
              </span>
            </div>
          </div>

          {/* Actions */}
          <div className="pt-6 flex flex-wrap gap-3">
            <Link href="/products" className="flex-1">
              <Button variant="outline" className="w-full">
                Tiếp tục mua sắm
              </Button>
            </Link>
            <Link href="/orders" className="flex-1">
              <Button className="w-full">
                Xem toàn bộ đơn hàng
              </Button>
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
