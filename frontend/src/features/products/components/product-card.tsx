"use client";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useCartStore } from "@/features/cart/store/useCartStore";
import { calculateDiscount, formatPrice } from "@/lib/utils";
import { Eye, ShoppingBag, Star, Zap } from "lucide-react";
import Image from "next/image";
import Link from "next/link";
import { Product } from "../types";

interface ProductCardProps {
  product: Product;
}

export function ProductCard({ product }: ProductCardProps) {
  const { addItem } = useCartStore();
  const discountPercent = calculateDiscount(product.price, product.discount_price);
  const currentPrice = product.discount_price || product.price;

  return (
    <div className="group relative flex flex-col bg-white dark:bg-slate-900 rounded-2xl border border-slate-100 dark:border-slate-800 overflow-hidden shadow-sm hover:shadow-xl hover:border-blue-500/30 transition-all duration-300">
      {/* Thumbnail & Badges */}
      <Link
        href={`/products/${product.id}`}
        className="relative aspect-square w-full overflow-hidden bg-slate-50 dark:bg-slate-800/40"
      >
        {product.thumbnail ? (
          <Image
            src={product.thumbnail}
            alt={product.name}
            fill
            className="object-cover group-hover:scale-105 transition-transform duration-500"
            sizes="(max-width: 768px) 100vw, (max-width: 1200px) 50vw, 25vw"
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center text-slate-300">
            Chưa có ảnh
          </div>
        )}

        {/* Badges */}
        <div className="absolute top-2.5 left-2.5 flex flex-col gap-1 z-10">
          {discountPercent > 0 && (
            <Badge variant="danger" className="font-bold shadow-sm">
              -{discountPercent}%
            </Badge>
          )}
          {product.brand && (
            <Badge variant="default" className="bg-white/90 dark:bg-slate-900/90 shadow-sm backdrop-blur-sm">
              {product.brand.name}
            </Badge>
          )}
        </div>

        {/* Views counter */}
        <div className="absolute bottom-2.5 right-2.5 z-10 flex items-center gap-1 bg-black/40 backdrop-blur-sm text-white px-2 py-0.5 rounded-full text-[10px] font-medium">
          <Eye className="w-3 h-3" />
          <span>{product.views}</span>
        </div>
      </Link>

      {/* Content */}
      <div className="p-4 flex-1 flex flex-col justify-between space-y-3">
        <div>
          {/* Rating */}
          <div className="flex items-center gap-1 text-amber-400 text-xs font-semibold mb-1.5">
            <Star className="w-3.5 h-3.5 fill-current" />
            <span>{product.rating || 5.0}</span>
            <span className="text-slate-400 text-[11px]">({product.views + 12} đánh giá)</span>
          </div>

          {/* Product Name */}
          <Link
            href={`/products/${product.id}`}
            className="block text-sm font-bold text-slate-900 dark:text-white line-clamp-2 hover:text-blue-600 transition"
          >
            {product.name}
          </Link>
        </div>

        {/* Price & Action */}
        <div className="pt-2 border-t border-slate-100 dark:border-slate-800/80">
          <div className="flex items-baseline gap-2">
            <span className="text-base font-extrabold text-blue-600 dark:text-blue-400">
              {formatPrice(currentPrice)}
            </span>
            {product.discount_price && product.discount_price < product.price && (
              <span className="text-xs text-slate-400 line-through">
                {formatPrice(product.price)}
              </span>
            )}
          </div>

          <div className="mt-3 flex items-center gap-2">
            <Button
              onClick={() => addItem(product, 1)}
              variant="primary"
              size="sm"
              className="flex-1 text-xs"
            >
              <ShoppingBag className="w-3.5 h-3.5" />
              Thêm giỏ hàng
            </Button>
            <Link
              href={`/products/${product.id}`}
              className="p-2 rounded-xl border border-slate-200 dark:border-slate-800 text-slate-500 hover:text-blue-600 hover:bg-slate-50 dark:hover:bg-slate-800 transition"
              title="Xem thông số"
            >
              <Zap className="w-4 h-4" />
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
