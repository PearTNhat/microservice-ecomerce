"use client";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { orderService } from "@/features/orders/services/order-service";
import { Order, OrderStatus } from "@/features/orders/types";
import { formatPrice } from "@/lib/utils";
import {
  AlertCircle,
  ArrowLeft,
  Calendar,
  ChevronRight,
  CreditCard,
  ExternalLink,
  Package,
  ShoppingBag,
} from "lucide-react";
import Image from "next/image";
import Link from "next/link";
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

export default function OrdersPage() {
  const [orders, setOrders] = useState<Order[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchOrders() {
      try {
        setIsLoading(true);
        setError(null);

        const token = localStorage.getItem("access_token");
        if (!token) {
          setError("Vui lòng đăng nhập để xem lịch sử đơn hàng của bạn.");
          setIsLoading(false);
          return;
        }

        const res = await orderService.getUserOrders(1, 20);
        if (res && res.data && res.data.orders) {
          setOrders(res.data.orders);
        }
      } catch (err: any) {
        setError(err.message || "Không thể tải danh sách đơn hàng");
      } finally {
        setIsLoading(false);
      }
    }

    fetchOrders();
  }, []);

  return (
    <div className="min-h-screen bg-slate-50/50 dark:bg-slate-950 py-10">
      <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="mb-8 flex flex-wrap items-center justify-between gap-4">
          <div>
            <Link
              href="/"
              className="inline-flex items-center text-xs font-semibold text-slate-500 hover:text-blue-600 mb-2 transition gap-1.5"
            >
              <ArrowLeft className="w-3.5 h-3.5" /> Về trang chủ
            </Link>
            <h1 className="text-2xl sm:text-3xl font-black text-slate-900 dark:text-white tracking-tight">
              Đơn Hàng Của Tôi
            </h1>
            <p className="text-sm text-slate-500 mt-1">
              Theo dõi và quản lý toàn bộ lịch sử mua sắm thiết bị điện máy của bạn.
            </p>
          </div>
          <Link href="/products">
            <Button variant="outline" size="sm">
              Tiếp tục mua sắm
            </Button>
          </Link>
        </div>

        {/* Loading State */}
        {isLoading && (
          <div className="space-y-4">
            {[1, 2, 3].map((i) => (
              <div
                key={i}
                className="bg-white dark:bg-slate-900 rounded-3xl p-6 shadow-sm border border-slate-100 dark:border-slate-800 animate-pulse space-y-4"
              >
                <div className="h-4 bg-slate-200 dark:bg-slate-800 rounded w-1/3" />
                <div className="h-16 bg-slate-100 dark:bg-slate-800/60 rounded-2xl" />
                <div className="h-4 bg-slate-200 dark:bg-slate-800 rounded w-1/4" />
              </div>
            ))}
          </div>
        )}

        {/* Error State */}
        {error && (
          <div className="bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-800/50 rounded-3xl p-8 text-center">
            <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-3" />
            <h3 className="text-base font-bold text-red-800 dark:text-red-300">{error}</h3>
            <div className="mt-4">
              <Link href="/login">
                <Button size="sm">Đăng nhập tài khoản</Button>
              </Link>
            </div>
          </div>
        )}

        {/* Empty State */}
        {!isLoading && !error && orders.length === 0 && (
          <div className="bg-white dark:bg-slate-900 rounded-3xl p-12 text-center shadow-sm border border-slate-100 dark:border-slate-800">
            <div className="w-16 h-16 rounded-2xl bg-blue-50 dark:bg-slate-800 text-blue-600 flex items-center justify-center mx-auto mb-4">
              <ShoppingBag className="w-8 h-8" />
            </div>
            <h3 className="text-lg font-bold text-slate-900 dark:text-white">
              Bạn chưa có đơn hàng nào
            </h3>
            <p className="text-sm text-slate-500 max-w-sm mx-auto mt-1 mb-6">
              Hãy khám phá các danh mục máy lạnh, tủ lạnh, máy giặt công nghệ mới ngay hôm nay!
            </p>
            <Link href="/products">
              <Button size="lg" className="shadow-lg shadow-blue-500/25">
                Khám phá sản phẩm
              </Button>
            </Link>
          </div>
        )}

        {/* Orders List */}
        {!isLoading && !error && orders.length > 0 && (
          <div className="space-y-4">
            {orders.map((order) => {
              const formattedDate = new Date(order.created_at).toLocaleDateString("vi-VN", {
                day: "2-digit",
                month: "2-digit",
                year: "numeric",
                hour: "2-digit",
                minute: "2-digit",
              });

              return (
                <div
                  key={order.id}
                  className="bg-white dark:bg-slate-900 rounded-3xl p-6 shadow-sm border border-slate-100 dark:border-slate-800 hover:shadow-md transition space-y-4"
                >
                  {/* Order Top Bar */}
                  <div className="flex flex-wrap items-center justify-between gap-3 pb-3 border-b border-slate-100 dark:border-slate-800 text-xs">
                    <div className="flex items-center gap-3">
                      <span className="font-mono font-bold text-slate-900 dark:text-white text-sm">
                        {order.order_code}
                      </span>
                      <span className="text-slate-400 flex items-center gap-1">
                        <Calendar className="w-3.5 h-3.5" /> {formattedDate}
                      </span>
                    </div>
                    <div>{getStatusBadge(order.order_status)}</div>
                  </div>

                  {/* Order Items Preview */}
                  <div className="space-y-2.5">
                    {order.items?.map((item, idx) => (
                      <div key={idx} className="flex items-center gap-3">
                        <div className="relative w-12 h-12 rounded-xl overflow-hidden bg-slate-50 dark:bg-slate-800 border border-slate-200/60 dark:border-slate-700 flex-shrink-0">
                          {item.thumbnail ? (
                            <Image
                              src={item.thumbnail}
                              alt={item.product_name}
                              fill
                              className="object-cover"
                              sizes="48px"
                            />
                          ) : (
                            <div className="w-full h-full flex items-center justify-center text-[10px] text-slate-400">
                              Ảnh
                            </div>
                          )}
                        </div>
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-semibold text-slate-800 dark:text-slate-200 line-clamp-1">
                            {item.product_name}
                          </p>
                          <p className="text-xs text-slate-400 mt-0.5">
                            {formatPrice(item.price)} × {item.quantity}
                          </p>
                        </div>
                        <div className="text-sm font-bold text-slate-900 dark:text-white">
                          {formatPrice(item.subtotal)}
                        </div>
                      </div>
                    ))}
                  </div>

                  {/* Order Bottom Bar */}
                  <div className="flex flex-wrap items-center justify-between gap-3 pt-3 border-t border-slate-100 dark:border-slate-800">
                    <div className="flex items-center gap-2 text-xs text-slate-500">
                      <CreditCard className="w-4 h-4 text-slate-400" />
                      <span>Hình thức: <strong className="text-slate-700 dark:text-slate-300">{order.payment_method}</strong></span>
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="text-right">
                        <span className="text-xs text-slate-400 block">Tổng thanh toán:</span>
                        <span className="text-lg font-black text-blue-600 dark:text-blue-400">
                          {formatPrice(order.total_amount)}
                        </span>
                      </div>
                      <Link href={`/orders/${order.id}`}>
                        <Button variant="outline" size="sm" className="gap-1 rounded-xl">
                          Chi tiết <ChevronRight className="w-3.5 h-3.5" />
                        </Button>
                      </Link>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
