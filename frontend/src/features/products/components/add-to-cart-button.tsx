"use client";

import { Button } from "@/components/ui/button";
import { useCartStore } from "@/features/cart/store/useCartStore";
import { Check, ShoppingBag } from "lucide-react";
import { useState } from "react";
import { Product } from "../types";

interface AddToCartButtonProps {
  product: Product;
}

export function AddToCartButton({ product }: AddToCartButtonProps) {
  const { addItem } = useCartStore();
  const [quantity, setQuantity] = useState(1);
  const [added, setAdded] = useState(false);

  const handleAdd = () => {
    addItem(product, quantity);
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
            onClick={() => setQuantity(quantity + 1)}
            className="w-8 h-8 flex items-center justify-center font-bold text-slate-600 hover:text-blue-600 rounded-lg"
          >
            +
          </button>
        </div>

        <span className="text-xs text-slate-500">
          Còn <strong className="text-slate-900 dark:text-white">{product.stock}</strong> sản phẩm trong kho
        </span>
      </div>

      <div className="flex flex-col sm:flex-row gap-3">
        <Button
          onClick={handleAdd}
          size="lg"
          variant="primary"
          className="flex-1 shadow-lg shadow-blue-500/25"
        >
          {added ? (
            <>
              <Check className="w-5 h-5 text-white" />
              Đã thêm vào giỏ hàng
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
