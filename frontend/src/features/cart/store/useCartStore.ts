import { ProductOfferResponse } from "@/features/flash-sale/types";
import { Product } from "@/features/products/types";
import { create } from "zustand";
import { persist } from "zustand/middleware";

export interface CartItem {
  product: Product;
  quantity: number;
  isFlashSale?: boolean;
  campaignId?: number;
  salePrice?: number;
  maxPerUser?: number;
}

interface CartStore {
  items: CartItem[];
  isOpen: boolean;
  openCart: () => void;
  closeCart: () => void;
  toggleCart: () => void;
  addItem: (
    product: Product,
    quantity?: number,
    flashSaleInfo?: {
      isFlashSale?: boolean;
      campaignId?: number;
      salePrice?: number;
      maxPerUser?: number;
    }
  ) => void;
  removeItem: (productId: number) => void;
  updateQuantity: (productId: number, quantity: number) => void;
  syncFlashSaleOffers: (offers: Record<number, ProductOfferResponse>) => void;
  clearCart: () => void;
  getTotalItems: () => number;
  getTotalPrice: () => number;
  hasFlashSaleItems: () => boolean;
}

export const useCartStore = create<CartStore>()(
  persist(
    (set, get) => ({
      items: [],
      isOpen: false,
      openCart: () => set({ isOpen: true }),
      closeCart: () => set({ isOpen: false }),
      toggleCart: () => set((state) => ({ isOpen: !state.isOpen })),
      addItem: (product, quantity = 1, flashSaleInfo) => {
        set((state) => {
          const existingItemIndex = state.items.findIndex(
            (item) => item.product.id === product.id
          );

          if (existingItemIndex > -1) {
            const newItems = [...state.items];
            newItems[existingItemIndex].quantity += quantity;
            if (flashSaleInfo?.isFlashSale) {
              newItems[existingItemIndex].isFlashSale = true;
              newItems[existingItemIndex].campaignId = flashSaleInfo.campaignId;
              newItems[existingItemIndex].salePrice = flashSaleInfo.salePrice;
              newItems[existingItemIndex].maxPerUser = flashSaleInfo.maxPerUser;
            }
            return { items: newItems, isOpen: true };
          }

          return {
            items: [
              ...state.items,
              {
                product,
                quantity,
                isFlashSale: flashSaleInfo?.isFlashSale,
                campaignId: flashSaleInfo?.campaignId,
                salePrice: flashSaleInfo?.salePrice,
                maxPerUser: flashSaleInfo?.maxPerUser,
              },
            ],
            isOpen: true,
          };
        });
      },
      removeItem: (productId) => {
        set((state) => ({
          items: state.items.filter((item) => item.product.id !== productId),
        }));
      },
      updateQuantity: (productId, quantity) => {
        if (quantity <= 0) {
          get().removeItem(productId);
          return;
        }
        set((state) => ({
          items: state.items.map((item) =>
            item.product.id === productId ? { ...item, quantity } : item
          ),
        }));
      },
      syncFlashSaleOffers: (offers) => {
        set((state) => {
          let hasChanges = false;
          const updatedItems = state.items.map((item) => {
            const offer = offers[item.product.id];
            const isSale = !!(offer && offer.has_flash_sale);
            const campId = isSale ? offer.campaign_id : undefined;
            const sPrice = isSale ? offer.sale_price : undefined;
            const maxQty = isSale ? offer.max_quantity_per_user : undefined;

            if (
              item.isFlashSale !== isSale ||
              item.campaignId !== campId ||
              item.salePrice !== sPrice ||
              item.maxPerUser !== maxQty
            ) {
              hasChanges = true;
              return {
                ...item,
                isFlashSale: isSale,
                campaignId: campId,
                salePrice: sPrice,
                maxPerUser: maxQty,
              };
            }
            return item;
          });

          if (!hasChanges) {
            return state;
          }
          return { items: updatedItems };
        });
      },
      clearCart: () => set({ items: [] }),
      getTotalItems: () => {
        return get().items.reduce((total, item) => total + item.quantity, 0);
      },
      getTotalPrice: () => {
        return get().items.reduce((total, item) => {
          const price =
            item.isFlashSale && item.salePrice
              ? item.salePrice
              : item.product.discount_price || item.product.price;
          return total + price * item.quantity;
        }, 0);
      },
      hasFlashSaleItems: () => {
        return get().items.some((item) => item.isFlashSale === true);
      },
    }),
    {
      name: "ecom_cart_storage",
    }
  )
);
