"use client";

import { Button } from "@/components/ui/button";
import { useCartStore } from "@/features/cart/store/useCartStore";
import { flashSaleService } from "@/features/flash-sale/services/flash-sale-service";
import { ProductOfferResponse } from "@/features/flash-sale/types";
import { Check, ShoppingBag, Zap } from "lucide-react";
import { useEffect, useState } from "react";
import { Product } from "../types";

interface AddToCartButtonProps {
  product: Product;
  initialOffer?: ProductOfferResponse | null;
}

export function AddToCartButton({ product, initialOffer }: AddToCartButtonProps) {
  const { addItem } = useCartStore();
  const [quantity, setQuantity] = useState(1);
  const [added, setAdded] = useState(false);
  const [offer, setOffer] = useState<ProductOfferResponse | null>(initialOffer || null);

  useEffect(() => {
    if (initialOffer !== undefined) return;
    let cancelled = false;
    flashSaleService
      .getProductOffer(product.id)
      .then((res) => {
        if (!cancelled && res.data && res.data.has_flash_sale) {
          setOffer(res.data);
        }
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [product.id, initialOffer]);

  const hasFlashSale = !!(offer && offer.has_flash_sale);
  const maxQuota = hasFlashSale && offer.max_quantity_per_user ? offer.max_quantity_per_user : product.stock;

  const handleAdd = () => {
    if (hasFlashSale) {
      addItem(product, quantity, {
        isFlashSale: true,
        campaignId: offer.campaign_id,
        salePrice: offer.sale_price,
        maxPerUser: offer.max_quantity_per_user,
      });
    } else {
      addItem(product, quantity);
    }
    setAdded(true);
    setTimeout(() => setAdded(false), 2000);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4">
        <div className="flex items-center border border-slate-200 dark:border-slate-700 rounded-xl bg-slate-50 dark:bg-slate-800 p-1">
          <button
            onClick={() => setQuantity(Math.max(1, quantity - 1))}
            className="w-8 h-8 flex items-center justify-center font-bold text-slate-600 hover:text-blue-600 rounded-lg"
          >
            -
          </button>
          <span className="w-10 text-center font-bold text-slate-900 dark:text-white text-sm">
            {quantity}
          </span>
          <button
            onClick={() => {
              if (maxQuota && quantity >= maxQuota) {
                alert(`Giới hạn mua tối đa ${maxQuota} sản phẩm!`);
                return;
              }
              setQuantity(quantity + 1);
            }}
            className="w-8 h-8 flex items-center justify-center font-bold text-slate-600 hover:text-blue-600 rounded-lg"
          >
            +
          </button>
        </div>

        <span className="text-xs text-slate-500">
          {hasFlashSale && offer.remaining_stock !== undefined ? (
            <span className="text-rose-600 dark:text-rose-400 font-semibold">
              Còn <strong>{offer.remaining_stock}</strong> suất Flash Sale (Tối đa {offer.max_quantity_per_user || 1} suất/khách)
            </span>
          ) : (
            <>
              Còn <strong className="text-slate-900 dark:text-white">{product.stock}</strong> sản phẩm trong kho
            </>
          )}
        </span>
      </div>

      <div className="flex flex-col sm:flex-row gap-3">
        <Button
          onClick={handleAdd}
          size="lg"
          variant="primary"
          className={`flex-1 shadow-lg ${
            hasFlashSale
              ? "bg-gradient-to-r from-red-600 via-rose-600 to-red-600 hover:from-red-700 hover:to-rose-700 shadow-rose-600/25 text-white"
              : "shadow-blue-500/25"
          }`}
        >
          {added ? (
            <>
              <Check className="w-5 h-5 text-white" />
              Đã thêm vào giỏ hàng
            </>
          ) : hasFlashSale ? (
            <>
              <Zap className="w-5 h-5 fill-current text-yellow-300" />
              Thêm Vào Giỏ (Giá Flash Sale)
            </>
          ) : (
            <>
              <ShoppingBag className="w-5 h-5" />
              Thêm Vào Giỏ Hàng
            </>
          )}
        </Button>
      </div>
    </div>
  );
}

