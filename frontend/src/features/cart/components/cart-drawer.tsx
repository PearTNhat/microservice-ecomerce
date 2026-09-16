"use client";

import { Button } from "@/components/ui/button";
import { flashSaleService } from "@/features/flash-sale/services/flash-sale-service";
import { formatPrice } from "@/lib/utils";
import { Minus, Plus, ShoppingBag, Trash2, X, Zap } from "lucide-react";
import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect } from "react";
import { useCartStore } from "../store/useCartStore";

export function CartDrawer() {
  const router = useRouter();
  const {
    items,
    isOpen,
    closeCart,
    updateQuantity,
    removeItem,
    getTotalPrice,
    getTotalItems,
    syncFlashSaleOffers,
    hasFlashSaleItems,
  } = useCartStore();

  // Tự động kiểm tra giá ưu đãi Flash Sale cho toàn bộ sản phẩm trong giỏ khi mở drawer
  useEffect(() => {
    if (!isOpen || items.length === 0) return;
    const ids = items.map((i) => i.product.id);
    flashSaleService
      .getBatchOffers(ids)
      .then((res) => {
        if (res.data && res.data.offers) {
          syncFlashSaleOffers(res.data.offers);
        }
      })
      .catch(() => {});
  }, [isOpen]);

  if (!isOpen) return null;

  const totalPrice = getTotalPrice();
  const totalItems = getTotalItems();
  const hasFlashSale = hasFlashSaleItems();

  return (
    <div className="fixed inset-0 z-50 overflow-hidden">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-slate-900/60 backdrop-blur-sm transition-opacity"
        onClick={closeCart}
      />

      <div className="fixed inset-y-0 right-0 max-w-full flex pl-10">
        <div className="w-screen max-w-md bg-white dark:bg-slate-900 shadow-2xl flex flex-col">
          {/* Header */}
          <div className="p-4 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <ShoppingBag className="w-5 h-5 text-blue-600" />
              <h2 className="text-lg font-bold text-slate-900 dark:text-white">
                Giỏ hàng ({totalItems})
              </h2>
            </div>
            <button
              onClick={closeCart}
              className="p-2 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-800 transition"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Banner thông báo nếu có Flash Sale trong giỏ */}
          {hasFlashSale && (
            <div className="bg-rose-50 dark:bg-rose-950/40 border-b border-rose-200/60 dark:border-rose-900/60 px-4 py-2.5 flex items-center gap-2 text-xs font-semibold text-rose-700 dark:text-rose-300">
              <Zap className="w-4 h-4 text-rose-600 fill-rose-600 animate-pulse flex-shrink-0" />
              <span>Giỏ hàng có ưu đãi Flash Sale! Thanh toán khi nhận hàng (COD).</span>
            </div>
          )}

          {/* Cart Items List */}
          <div className="flex-1 overflow-y-auto p-4 space-y-4">
            {items.length === 0 ? (
              <div className="h-full flex flex-col items-center justify-center text-center p-6 text-slate-400">
                <ShoppingBag className="w-16 h-16 stroke-[1.5] mb-4 text-slate-300 dark:text-slate-700" />
                <p className="text-base font-medium text-slate-700 dark:text-slate-300">
                  Giỏ hàng của bạn đang trống
                </p>
                <p className="text-sm mt-1 mb-6">Hãy khám phá các thiết bị điện máy hấp dẫn ngay!</p>
                <Button onClick={closeCart} variant="outline" size="sm">
                  Tiếp tục mua sắm
                </Button>
              </div>
            ) : (
              items.map(({ product, quantity, isFlashSale, salePrice, maxPerUser }) => {
                const currentPrice =
                  isFlashSale && salePrice
                    ? salePrice
                    : product.discount_price || product.price;

                const originalPrice = product.price;

                return (
                  <div
                    key={product.id}
                    className={`flex gap-3 p-3 rounded-2xl border transition ${
                      isFlashSale
                        ? "bg-rose-50/40 dark:bg-rose-950/20 border-rose-200 dark:border-rose-900/50"
                        : "bg-slate-50 dark:bg-slate-800/50 border-slate-100 dark:border-slate-800"
                    }`}
                  >
                    <div className="relative w-20 h-20 rounded-xl overflow-hidden bg-white dark:bg-slate-900 border border-slate-200/60 dark:border-slate-800 flex-shrink-0">
                      {product.thumbnail ? (
                        <Image
                          src={product.thumbnail}
                          alt={product.name}
                          fill
                          className="object-cover"
                          sizes="80px"
                        />
                      ) : (
                        <div className="w-full h-full flex items-center justify-center text-slate-300">
                          Ảnh
                        </div>
                      )}
                      {isFlashSale && (
                        <div className="absolute top-1 left-1 bg-rose-600 text-white text-[9px] font-black px-1.5 py-0.5 rounded shadow">
                          FLASH
                        </div>
                      )}
                    </div>

                    <div className="flex-1 min-w-0 flex flex-col justify-between">
                      <div>
                        <Link
                          href={`/products/${product.id}`}
                          onClick={closeCart}
                          className="text-sm font-semibold text-slate-900 dark:text-white line-clamp-2 hover:text-blue-600 transition"
                        >
                          {product.name}
                        </Link>

                        <div className="flex items-baseline gap-2 mt-1">
                          <p
                            className={`text-xs font-bold ${
                              isFlashSale
                                ? "text-rose-600 dark:text-rose-400"
                                : "text-blue-600 dark:text-blue-400"
                            }`}
                          >
                            {formatPrice(currentPrice)}
                          </p>
                          {isFlashSale && originalPrice > currentPrice && (
                            <span className="text-[10px] text-slate-400 line-through">
                              {formatPrice(originalPrice)}
                            </span>
                          )}
                        </div>

                        {isFlashSale && maxPerUser && (
                          <p className="text-[10px] text-amber-600 dark:text-amber-400 mt-0.5 font-medium">
                            Tối đa {maxPerUser} suất giá sốc/khách
                          </p>
                        )}
                      </div>

                      <div className="flex items-center justify-between mt-2">
                        <div className="flex items-center border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 overflow-hidden">
                          <button
                            onClick={() => updateQuantity(product.id, quantity - 1)}
                            className="p-1 hover:bg-slate-100 dark:hover:bg-slate-800 text-slate-500"
                          >
                            <Minus className="w-3.5 h-3.5" />
                          </button>
                          <span className="px-2 text-xs font-semibold">{quantity}</span>
                          <button
                            onClick={() => {
                              if (isFlashSale && maxPerUser && quantity >= maxPerUser) {
                                alert(
                                  `Sản phẩm này trong đợt Flash Sale chỉ được mua tối đa ${maxPerUser} suất ưu đãi!`
                                );
                                return;
                              }
                              updateQuantity(product.id, quantity + 1);
                            }}
                            className="p-1 hover:bg-slate-100 dark:hover:bg-slate-800 text-slate-500"
                          >
                            <Plus className="w-3.5 h-3.5" />
                          </button>
                        </div>

                        <button
                          onClick={() => removeItem(product.id)}
                          className="text-slate-400 hover:text-red-500 p-1 transition"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </div>
                  </div>
                );
              })
            )}
          </div>

          {/* Footer & Checkout button */}
          {items.length > 0 && (
            <div className="p-4 border-t border-slate-100 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-900/50 space-y-3">
              <div className="flex items-center justify-between text-sm">
                <span className="text-slate-500">Tạm tính:</span>
                <span className="text-lg font-bold text-slate-900 dark:text-white">
                  {formatPrice(totalPrice)}
                </span>
              </div>
              <p className="text-xs text-slate-400">
                Đã bao gồm VAT & Miễn phí vận chuyển toàn quốc
              </p>
              <Button
                className={`w-full shadow-lg ${
                  hasFlashSale
                    ? "bg-gradient-to-r from-rose-600 to-red-600 hover:from-rose-700 hover:to-red-700 shadow-rose-600/20"
                    : "shadow-blue-500/25"
                }`}
                size="lg"
                onClick={() => {
                  closeCart();
                  router.push("/checkout");
                }}
              >
                {hasFlashSale ? "⚡ Tiến hành Đặt Hàng Flash Sale" : "Tiến hành Đặt Hàng"}
              </Button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

