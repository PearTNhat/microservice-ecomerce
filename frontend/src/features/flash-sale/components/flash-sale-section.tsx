"use client";

import { Button } from "@/components/ui/button";
import { ActiveCampaign, ActiveCampaignItem } from "@/features/flash-sale/types";
import { flashSaleService } from "@/features/flash-sale/services/flash-sale-service";
import { Product } from "@/features/products/types";
import { formatPrice } from "@/lib/utils";
import { Flame, Sparkles, Timer, Zap, ShieldCheck } from "lucide-react";
import Image from "next/image";
import React, { useEffect, useState } from "react";
import { FlashSaleModal } from "./flash-sale-modal";

interface FlashSaleSectionProps {
  products?: Product[];
}

export const FlashSaleSection: React.FC<FlashSaleSectionProps> = ({
  products = [],
}) => {
  const [activeCampaign, setActiveCampaign] = useState<ActiveCampaign | null>(null);
  const [selectedItem, setSelectedItem] = useState<ActiveCampaignItem | null>(null);
  const [selectedProduct, setSelectedProduct] = useState<Product | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [loading, setLoading] = useState(true);

  // Live countdown timer
  const [timeLeft, setTimeLeft] = useState({
    hours: 2,
    minutes: 0,
    seconds: 0,
  });

  // 1. Fetch Active Flash Sale Campaign from Backend (Auto-poll every 5s if none active)
  useEffect(() => {
    let mounted = true;

    const fetchActiveCampaign = async () => {
      try {
        const res = await flashSaleService.getActiveCampaign();
        if (mounted && res.data && res.data.items && res.data.items.length > 0) {
          setActiveCampaign(res.data);
        } else if (mounted) {
          setActiveCampaign(null);
        }
      } catch (err) {
        // Fallback to static products if backend has no active campaign
      } finally {
        if (mounted) setLoading(false);
      }
    };

    fetchActiveCampaign();
    const interval = setInterval(fetchActiveCampaign, 6000);

    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, []);

  // 2. Countdown Clock Logic (synchronized with activeCampaign.ends_at if available)
  useEffect(() => {
    const updateCountdown = () => {
      if (activeCampaign && activeCampaign.ends_at) {
        const endEpoch = new Date(activeCampaign.ends_at).getTime();
        const nowEpoch = Date.now();
        const diffSec = Math.max(0, Math.floor((endEpoch - nowEpoch) / 1000));

        const hours = Math.floor(diffSec / 3600);
        const minutes = Math.floor((diffSec % 3600) / 60);
        const seconds = diffSec % 60;

        setTimeLeft({ hours, minutes, seconds });
      } else {
        // Default loop countdown when using static fallback
        setTimeLeft((prev) => {
          if (prev.seconds > 0) {
            return { ...prev, seconds: prev.seconds - 1 };
          }
          if (prev.minutes > 0) {
            return { ...prev, minutes: prev.minutes - 1, seconds: 59 };
          }
          if (prev.hours > 0) {
            return { hours: prev.hours - 1, minutes: 59, seconds: 59 };
          }
          return { hours: 2, minutes: 30, seconds: 0 };
        });
      }
    };

    updateCountdown();
    const timer = setInterval(updateCountdown, 1000);
    return () => clearInterval(timer);
  }, [activeCampaign]);

  const handleOpenBuyItem = (item: ActiveCampaignItem) => {
    setSelectedItem(item);
    setSelectedProduct(null);
    setModalOpen(true);
  };

  const handleOpenBuyProductFallback = (p: Product) => {
    setSelectedProduct(p);
    setSelectedItem(null);
    setModalOpen(true);
  };

  const hasActiveCampaign = !!(activeCampaign && activeCampaign.items && activeCampaign.items.length > 0);
  const fallbackItems = products.slice(0, 4);

  // If no active campaign and no fallback products, do not render
  if (!hasActiveCampaign && fallbackItems.length === 0 && !loading) {
    return null;
  }

  return (
    <section className="relative overflow-hidden rounded-3xl bg-gradient-to-b from-slate-900 via-rose-950/40 to-slate-900 border border-rose-500/30 p-6 sm:p-8 shadow-2xl">
      {/* Background glow effects */}
      <div className="absolute top-0 left-1/4 w-96 h-96 bg-rose-500/10 rounded-full blur-3xl pointer-events-none" />
      <div className="absolute bottom-0 right-1/4 w-96 h-96 bg-amber-500/10 rounded-full blur-3xl pointer-events-none" />

      {/* Header with Title and Countdown */}
      <div className="relative z-10 flex flex-col md:flex-row md:items-center justify-between gap-4 pb-6 border-b border-rose-500/20">
        <div className="space-y-1">
          <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-rose-500/20 border border-rose-500/40 text-rose-300 text-xs font-black uppercase tracking-wider">
            <Flame className="w-4 h-4 text-amber-400 fill-amber-400 animate-bounce" />
            {hasActiveCampaign ? (
              <span>CHIẾN DỊCH: {activeCampaign.name}</span>
            ) : (
              <span>GIỜ VÀNG GIÁ SỐC • SỐ LƯỢNG CÓ HẠN</span>
            )}
          </div>
          <h2 className="text-2xl sm:text-3xl font-black text-white tracking-tight flex items-center gap-2">
            ⚡ FLASH SALE HÔM NAY
          </h2>
        </div>

        {/* Live Countdown Clock */}
        <div className="flex items-center gap-2 bg-slate-950/80 px-4 py-2.5 rounded-2xl border border-rose-500/30 shadow-inner">
          <div className="flex items-center gap-1.5 text-xs font-bold text-slate-400 uppercase mr-1">
            <Timer className="w-4 h-4 text-rose-400" />
            <span>Kết thúc trong:</span>
          </div>
          <div className="flex items-center gap-1 font-mono text-sm font-black">
            <span className="bg-rose-600 text-white px-2 py-1 rounded-lg shadow-sm">
              {String(timeLeft.hours).padStart(2, "0")}
            </span>
            <span className="text-rose-400 font-bold">:</span>
            <span className="bg-rose-600 text-white px-2 py-1 rounded-lg shadow-sm">
              {String(timeLeft.minutes).padStart(2, "0")}
            </span>
            <span className="text-rose-400 font-bold">:</span>
            <span className="bg-rose-600 text-white px-2 py-1 rounded-lg shadow-sm">
              {String(timeLeft.seconds).padStart(2, "0")}
            </span>
          </div>
        </div>
      </div>

      {/* Flash Sale Product Cards Grid */}
      <div className="relative z-10 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 pt-6">
        {hasActiveCampaign ? (
          // Render Live Active Campaign Items
          activeCampaign.items.map((item) => {
            const soldPercent = item.allocated_stock > 0
              ? Math.min(100, Math.round((item.sold_stock / item.allocated_stock) * 100))
              : 0;

            const isOneTime = item.max_quantity_per_user === 1;

            return (
              <div
                key={item.id}
                className="group relative bg-slate-900/90 rounded-2xl border border-slate-800 hover:border-rose-500/60 p-4 transition-all duration-300 hover:shadow-xl hover:shadow-rose-500/10 flex flex-col justify-between"
              >
                {/* Image & Discount Badge */}
                <div className="relative w-full aspect-square rounded-xl overflow-hidden bg-slate-800/80 mb-3">
                  <Image
                    src={item.product_thumbnail || "/placeholder.png"}
                    alt={item.product_name}
                    fill
                    sizes="(max-width: 768px) 100vw, (max-width: 1200px) 50vw, 25vw"
                    className="object-cover group-hover:scale-105 transition-transform duration-300"
                  />
                  {item.discount_percentage > 0 && (
                    <div className="absolute top-2 left-2 bg-gradient-to-r from-red-600 to-rose-600 text-white text-xs font-black px-2 py-0.5 rounded-md shadow-md">
                      -{item.discount_percentage}%
                    </div>
                  )}
                  <div className="absolute top-2 right-2 bg-black/60 backdrop-blur-sm text-yellow-300 text-[10px] font-bold px-1.5 py-0.5 rounded border border-yellow-400/20 flex items-center gap-1">
                    {isOneTime ? "1 Suất/Khách" : `Tối đa ${item.max_quantity_per_user}`}
                  </div>
                  <div className="absolute bottom-2 right-2 bg-black/60 backdrop-blur-sm text-yellow-300 text-[10px] font-bold px-1.5 py-0.5 rounded border border-yellow-400/20 flex items-center gap-1">
                    <Zap className="w-3 h-3 fill-current" />
                    Flash Deal
                  </div>
                </div>

                {/* Title & Pricing */}
                <div className="space-y-2 flex-1 flex flex-col justify-between">
                  <div>
                    <h3 className="font-bold text-sm text-slate-100 line-clamp-2 group-hover:text-rose-400 transition-colors">
                      {item.product_name}
                    </h3>
                    <div className="flex items-baseline gap-2 mt-1.5 flex-wrap">
                      <span className="text-lg font-black text-rose-400">
                        {formatPrice(item.sale_price)}
                      </span>
                      {item.original_price > item.sale_price && (
                        <span className="text-xs text-slate-400 line-through">
                          {formatPrice(item.original_price)}
                        </span>
                      )}
                    </div>
                  </div>

                  {/* Stock Progress Bar */}
                  <div className="space-y-1.5 pt-2">
                    <div className="flex justify-between items-center text-[11px] font-bold">
                      <span className="text-amber-400 flex items-center gap-1">
                        <Flame className="w-3 h-3 fill-amber-400" />
                        Đã bán {soldPercent}%
                      </span>
                      <span className={`font-mono ${item.remaining_stock > 0 ? "text-amber-300" : "text-rose-400"}`}>
                        {item.remaining_stock > 0 ? `Còn ${item.remaining_stock} suất` : "Hết hàng"}
                      </span>
                    </div>
                    <div className="w-full bg-slate-800 rounded-full h-2 overflow-hidden border border-slate-700/60">
                      <div
                        className="bg-gradient-to-r from-amber-500 via-rose-500 to-red-600 h-full rounded-full animate-pulse"
                        style={{ width: `${Math.max(5, soldPercent)}%` }}
                      />
                    </div>
                  </div>

                  {/* Action Button */}
                  <div className="pt-3">
                    <Button
                      disabled={item.remaining_stock <= 0}
                      onClick={() => handleOpenBuyItem(item)}
                      className="w-full bg-gradient-to-r from-amber-500 via-rose-600 to-red-600 hover:from-amber-600 hover:to-red-700 disabled:opacity-50 text-white font-black text-xs h-10 rounded-xl shadow-md shadow-rose-600/20 gap-1.5 group-hover:shadow-rose-600/40"
                    >
                      <Zap className="w-4 h-4 fill-current" />
                      {item.remaining_stock > 0 ? "SĂN NGAY" : "HẾT SUẤT"}
                    </Button>
                  </div>
                </div>
              </div>
            );
          })
        ) : (
          // Fallback static items
          fallbackItems.map((product, idx) => {
            const discountPercent =
              product.discount_price && product.discount_price < product.price
                ? Math.round(((product.price - product.discount_price) / product.price) * 100)
                : 35;

            const currentPrice = product.discount_price || product.price * 0.7;
            const soldPercent = 65 + ((idx * 9) % 30);

            return (
              <div
                key={product.id}
                className="group relative bg-slate-900/90 rounded-2xl border border-slate-800 hover:border-rose-500/60 p-4 transition-all duration-300 hover:shadow-xl hover:shadow-rose-500/10 flex flex-col justify-between"
              >
                <div className="relative w-full aspect-square rounded-xl overflow-hidden bg-slate-800/80 mb-3">
                  <Image
                    src={product.thumbnail || "/placeholder.png"}
                    alt={product.name}
                    fill
                    sizes="(max-width: 768px) 100vw, (max-width: 1200px) 50vw, 25vw"
                    className="object-cover group-hover:scale-105 transition-transform duration-300"
                  />
                  <div className="absolute top-2 left-2 bg-gradient-to-r from-red-600 to-rose-600 text-white text-xs font-black px-2 py-0.5 rounded-md shadow-md">
                    -{discountPercent}%
                  </div>
                  <div className="absolute bottom-2 right-2 bg-black/60 backdrop-blur-sm text-yellow-300 text-[10px] font-bold px-1.5 py-0.5 rounded border border-yellow-400/20 flex items-center gap-1">
                    <Zap className="w-3 h-3 fill-current" />
                    Flash Deal
                  </div>
                </div>

                <div className="space-y-2 flex-1 flex flex-col justify-between">
                  <div>
                    <h3 className="font-bold text-sm text-slate-100 line-clamp-2 group-hover:text-rose-400 transition-colors">
                      {product.name}
                    </h3>
                    <div className="flex items-baseline gap-2 mt-1.5">
                      <span className="text-lg font-black text-rose-400">
                        {formatPrice(currentPrice)}
                      </span>
                      <span className="text-xs text-slate-400 line-through">
                        {formatPrice(product.price)}
                      </span>
                    </div>
                  </div>

                  <div className="space-y-1.5 pt-2">
                    <div className="flex justify-between items-center text-[11px] font-bold">
                      <span className="text-amber-400 flex items-center gap-1">
                        <Flame className="w-3 h-3 fill-amber-400" />
                        Đã bán {soldPercent}%
                      </span>
                      <span className={`font-mono ${product.stock > 0 ? "text-amber-300" : "text-rose-400"}`}>
                        {product.stock > 0 ? `Còn ${product.stock} chiếc` : "Hết hàng"}
                      </span>
                    </div>
                    <div className="w-full bg-slate-800 rounded-full h-2 overflow-hidden border border-slate-700/60">
                      <div
                        className="bg-gradient-to-r from-amber-500 via-rose-500 to-red-600 h-full rounded-full animate-pulse"
                        style={{ width: `${soldPercent}%` }}
                      />
                    </div>
                  </div>

                  <div className="pt-3">
                    <Button
                      disabled
                      className="w-full bg-slate-800/90 border border-slate-700 text-slate-400 font-bold text-xs h-10 rounded-xl gap-1.5 cursor-not-allowed opacity-80"
                    >
                      <Timer className="w-3.5 h-3.5 text-amber-400" />
                      SẮP MỞ BÁN
                    </Button>
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>

      {/* Modal Quick Purchase */}
      <FlashSaleModal
        campaignId={activeCampaign?.id}
        item={selectedItem}
        product={selectedProduct}
        isOpen={modalOpen}
        onClose={() => {
          setModalOpen(false);
          setSelectedItem(null);
          setSelectedProduct(null);
        }}
      />
    </section>
  );
};
