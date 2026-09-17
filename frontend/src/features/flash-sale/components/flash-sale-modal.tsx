"use client";

import { Button } from "@/components/ui/button";
import { useCartStore } from "@/features/cart/store/useCartStore";
import { ActiveCampaignItem } from "@/features/flash-sale/types";
import { Product } from "@/features/products/types";
import { formatPrice } from "@/lib/utils";
import {
  Check,
  Flame,
  Minus,
  Plus,
  ShieldCheck,
  ShoppingBag,
  Sparkles,
  Truck,
  X,
  Zap,
} from "lucide-react";
import Image from "next/image";
import { useRouter } from "next/navigation";
import React, { useEffect, useState } from "react";

interface FlashSaleModalProps {
  campaignId?: number | null;
  item?: ActiveCampaignItem | null;
  product?: Product | null;
  isOpen: boolean;
  onClose: () => void;
}

export const FlashSaleModal: React.FC<FlashSaleModalProps> = ({
  campaignId,
  item,
  product,
  isOpen,
  onClose,
}) => {
  const router = useRouter();
  const { addItem, openCart, setDirectCheckoutDraft } = useCartStore();

  const [quantity, setQuantity] = useState(1);
  const [justAdded, setJustAdded] = useState(false);

  // Unified Product Information (from ActiveCampaignItem or fallback Product)
  const displayInfo = {
    id: item ? item.product_id : product?.id || 0,
    name: item ? item.product_name : product?.name || "",
    thumbnail: item?.product_thumbnail || product?.thumbnail || "/placeholder.png",
    currentPrice: item ? item.sale_price : product?.discount_price || product?.price || 0,
    originalPrice: item ? item.original_price : product?.price || 0,
    discountPercent: item
      ? item.discount_percentage
      : product && product.discount_price && product.discount_price < product.price
      ? Math.round(((product.price - product.discount_price) / product.price) * 100)
      : 0,
    stockLeft: item ? item.remaining_stock : product?.stock || 0,
    maxPerUser: item?.max_quantity_per_user || 5,
  };

  const maxAllowedQty = Math.max(
    1,
    Math.min(displayInfo.maxPerUser, displayInfo.stockLeft > 0 ? displayInfo.stockLeft : 1)
  );

  useEffect(() => {
    if (isOpen) {
      setQuantity(1);
      setJustAdded(false);
    }
  }, [isOpen, item, product]);

  if (!isOpen || (!item && !product)) return null;

  // Resolve standard Product object for cart store
  const resolvedProduct: Product = product || {
    id: displayInfo.id,
    name: displayInfo.name,
    slug: `product-${displayInfo.id}`,
    price: displayInfo.originalPrice || displayInfo.currentPrice,
    discount_price: displayInfo.currentPrice,
    stock: displayInfo.stockLeft,
    thumbnail: displayInfo.thumbnail,
    rating: 5,
    views: 100,
  };

  const flashSaleMeta = {
    isFlashSale: true,
    campaignId: (campaignId || item?.campaign_id) ?? undefined,
    salePrice: displayInfo.currentPrice,
    maxPerUser: displayInfo.maxPerUser,
  };

  const handleAddToCart = () => {
    addItem(resolvedProduct, quantity, flashSaleMeta);
    setJustAdded(true);
    setTimeout(() => {
      setJustAdded(false);
      onClose();
      openCart();
    }, 600);
  };

  const handleBuyNow = () => {
    // 10.5: Mua ngay lưu vào directCheckoutDraft và mở /checkout?mode=direct,
    // không gộp hay làm mất các món hàng khác đang có trong giỏ hàng chính (items).
    setDirectCheckoutDraft([
      {
        product: resolvedProduct,
        quantity,
        isFlashSale: flashSaleMeta.isFlashSale,
        campaignId: flashSaleMeta.campaignId,
        salePrice: flashSaleMeta.salePrice,
        maxPerUser: flashSaleMeta.maxPerUser,
      },
    ]);
    onClose();
    router.push("/checkout?mode=direct");
  };

  const savings = Math.max(0, displayInfo.originalPrice - displayInfo.currentPrice);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-md animate-in fade-in duration-200">
      <div className="relative w-full max-w-lg overflow-hidden rounded-3xl bg-slate-900 border border-rose-500/30 p-6 sm:p-7 shadow-2xl shadow-rose-950/50 text-white">
        {/* Glow ambient */}
        <div className="absolute top-0 left-1/2 -translate-x-1/2 w-80 h-32 bg-gradient-to-b from-rose-500/20 to-transparent blur-2xl pointer-events-none" />

        {/* Header */}
        <div className="relative flex items-center justify-between pb-4 border-b border-rose-500/20">
          <div className="flex items-center gap-2">
            <span className="p-2 rounded-xl bg-rose-500/20 border border-rose-500/40 text-rose-400">
              <Zap className="w-5 h-5 fill-current animate-pulse" />
            </span>
            <div>
              <div className="flex items-center gap-1.5 text-xs font-black uppercase tracking-wider text-rose-400">
                <Flame className="w-3.5 h-3.5 text-amber-400 fill-amber-400" />
                ƯU ĐÃI FLASH SALE ĐỘC QUYỀN
              </div>
              <h3 className="text-base sm:text-lg font-bold text-white">
                Chọn số lượng & Đặt hàng
              </h3>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-full hover:bg-slate-800 text-slate-400 hover:text-white transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Product Showcase */}
        <div className="relative mt-5 flex gap-4 p-4 rounded-2xl bg-slate-950/70 border border-slate-800 shadow-inner">
          <div className="relative w-24 h-24 sm:w-28 sm:h-28 shrink-0 rounded-xl overflow-hidden bg-slate-800 border border-slate-700">
            <Image
              src={displayInfo.thumbnail}
              alt={displayInfo.name}
              fill
              sizes="112px"
              className="object-cover"
            />
            {displayInfo.discountPercent > 0 && (
              <div className="absolute top-1 left-1 bg-gradient-to-r from-red-600 to-rose-600 text-white text-[10px] font-black px-1.5 py-0.5 rounded shadow">
                -{displayInfo.discountPercent}%
              </div>
            )}
          </div>

          <div className="flex flex-col justify-between flex-1 min-w-0">
            <div>
              <h4 className="font-bold text-white text-sm sm:text-base line-clamp-2">
                {displayInfo.name}
              </h4>
              <div className="mt-2 flex items-baseline gap-2 flex-wrap">
                <span className="text-2xl font-black text-rose-400 tracking-tight">
                  {formatPrice(displayInfo.currentPrice)}
                </span>
                {displayInfo.originalPrice > displayInfo.currentPrice && (
                  <span className="text-xs line-through text-slate-400 font-medium">
                    {formatPrice(displayInfo.originalPrice)}
                  </span>
                )}
              </div>
              {savings > 0 && (
                <div className="inline-flex items-center gap-1 mt-1 text-[11px] font-bold text-amber-400">
                  <Sparkles className="w-3 h-3" />
                  Tiết kiệm {formatPrice(savings)} / món
                </div>
              )}
            </div>

            <div className="mt-2 text-[11px] text-slate-400 flex items-center justify-between">
              <span>Còn lại: <strong className="text-rose-300">{displayInfo.stockLeft}</strong> suất</span>
              <span className="text-amber-400 font-semibold">Tối đa {displayInfo.maxPerUser} món/khách</span>
            </div>
          </div>
        </div>

        {/* Quantity Selection */}
        <div className="mt-5 p-4 rounded-2xl bg-slate-800/40 border border-slate-700/60 flex items-center justify-between">
          <div>
            <div className="text-xs font-bold uppercase text-slate-300">
              Số lượng mua
            </div>
            <div className="text-[11px] text-slate-400">
              Áp dụng mức giá Flash Sale tốt nhất
            </div>
          </div>

          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={() => setQuantity((q) => Math.max(1, q - 1))}
              disabled={quantity <= 1}
              className="w-9 h-9 rounded-xl bg-slate-800 border border-slate-700 flex items-center justify-center text-slate-200 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition"
            >
              <Minus className="w-4 h-4" />
            </button>
            <span className="w-8 text-center font-mono font-black text-lg text-white">
              {quantity}
            </span>
            <button
              type="button"
              onClick={() => setQuantity((q) => Math.min(maxAllowedQty, q + 1))}
              disabled={quantity >= maxAllowedQty}
              className="w-9 h-9 rounded-xl bg-slate-800 border border-slate-700 flex items-center justify-center text-slate-200 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition"
            >
              <Plus className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Total Price Summary */}
        <div className="mt-4 flex items-center justify-between px-2 text-sm">
          <span className="text-slate-400 font-medium">Tạm tính ưu đãi:</span>
          <span className="text-xl font-black text-rose-400 font-mono">
            {formatPrice(displayInfo.currentPrice * quantity)}
          </span>
        </div>

        {/* Actions */}
        <div className="mt-5 space-y-2.5">
          <Button
            type="button"
            onClick={handleBuyNow}
            size="lg"
            className="w-full bg-gradient-to-r from-amber-500 via-red-500 to-rose-600 hover:from-amber-600 hover:to-rose-700 text-white font-black text-base shadow-xl shadow-rose-600/30 gap-2 h-12 rounded-xl"
          >
            <Zap className="w-5 h-5 fill-current" />
            MUA NGAY (TIẾN HÀNH ĐẶT HÀNG)
          </Button>

          <Button
            type="button"
            variant="outline"
            onClick={handleAddToCart}
            disabled={justAdded}
            className="w-full bg-slate-800/80 border-slate-700 hover:bg-slate-800 text-slate-200 font-bold h-11 rounded-xl gap-2 transition"
          >
            {justAdded ? (
              <>
                <Check className="w-4 h-4 text-emerald-400" />
                <span className="text-emerald-400">Đã thêm vào giỏ hàng!</span>
              </>
            ) : (
              <>
                <ShoppingBag className="w-4 h-4 text-slate-300" />
                Thêm Vào Giỏ Hàng (Mua Cùng Món Khác)
              </>
            )}
          </Button>
        </div>

        {/* Guarantees */}
        <div className="mt-5 pt-4 border-t border-slate-800/80 grid grid-cols-3 gap-2 text-center text-[10px] sm:text-[11px] text-slate-400">
          <div className="flex flex-col items-center gap-1">
            <ShieldCheck className="w-4 h-4 text-emerald-400" />
            <span>Chính hãng 100%</span>
          </div>
          <div className="flex flex-col items-center gap-1">
            <Zap className="w-4 h-4 text-amber-400" />
            <span>Giữ giá tại Checkout</span>
          </div>
          <div className="flex flex-col items-center gap-1">
            <Truck className="w-4 h-4 text-rose-400" />
            <span>Giao toàn quốc</span>
          </div>
        </div>
      </div>
    </div>
  );
};
