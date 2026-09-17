"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { flashSaleService } from "@/features/flash-sale/services/flash-sale-service";
import { AdminCampaignItem } from "@/features/flash-sale/types";
import { Loader2, Pencil, ShieldCheck, Users, X } from "lucide-react";
import React, { useEffect, useState } from "react";

interface EditItemModalProps {
  data: { campaignId: number; item: AdminCampaignItem } | null;
  onClose: () => void;
  onSuccess: (message: string) => void;
  onError: (error: string) => void;
}

export function EditItemModal({
  data,
  onClose,
  onSuccess,
  onError,
}: EditItemModalProps) {
  const [salePrice, setSalePrice] = useState("");
  const [originalPrice, setOriginalPrice] = useState("");
  const [allocatedStock, setAllocatedStock] = useState("");
  const [quotaType, setQuotaType] = useState<"SINGLE" | "MULTIPLE">("SINGLE");
  const [maxUser, setMaxUser] = useState("2");
  const [resvSec, setResvSec] = useState("120");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (data?.item) {
      const it = data.item;
      setSalePrice(String(it.sale_price));
      setOriginalPrice(String(it.original_price));
      setAllocatedStock(String(it.allocated_stock));
      setQuotaType(it.max_quantity_per_user === 1 ? "SINGLE" : "MULTIPLE");
      setMaxUser(String(it.max_quantity_per_user || 2));
      setResvSec(String(it.reservation_seconds || 120));
    }
  }, [data]);

  if (!data) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setSubmitting(true);
      const maxUserNum = quotaType === "SINGLE" ? 1 : Math.max(1, parseInt(maxUser) || 2);
      await flashSaleService.updateCampaignItem(data.campaignId, data.item.id, {
        sale_price: parseFloat(salePrice) || 0,
        original_price: parseFloat(originalPrice) || 0,
        allocated_stock: parseInt(allocatedStock) || 1,
        max_quantity_per_user: maxUserNum,
        max_quantity_per_order: 1,
        reservation_seconds: parseInt(resvSec) || 120,
      });

      onSuccess(`Cập nhật sản phẩm #${data.item.product_id} thành công!`);
      onClose();
    } catch (err: any) {
      onError(err?.message || "Cập nhật sản phẩm thất bại");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-200">
      <div className="relative w-full max-w-lg bg-slate-900 border border-amber-500/30 rounded-3xl shadow-2xl overflow-hidden text-slate-100 flex flex-col max-h-[90vh]">
        <div className="p-5 bg-gradient-to-r from-amber-600 to-rose-600 text-white flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <div className="p-2 bg-white/20 rounded-xl">
              <Pencil className="w-5 h-5 text-white" />
            </div>
            <div>
              <h3 className="text-lg font-black tracking-tight">Sửa Sản Phẩm #{data.item.product_id}</h3>
              <p className="text-xs text-amber-100">Cập nhật giá flash sale, phân bổ kho và hạn mức mua</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-full hover:bg-white/20 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-6 overflow-y-auto space-y-4 text-xs">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Số lượng phân bổ *</label>
              <Input
                required
                type="number"
                min="1"
                value={allocatedStock}
                onChange={(e) => setAllocatedStock(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono"
              />
            </div>
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Giữ chỗ (Giây) *</label>
              <Input
                required
                type="number"
                min="30"
                max="600"
                value={resvSec}
                onChange={(e) => setResvSec(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono"
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Giá gốc (VNĐ) *</label>
              <Input
                required
                type="number"
                min="1000"
                value={originalPrice}
                onChange={(e) => setOriginalPrice(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono"
              />
            </div>
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Giá Flash Sale (VNĐ) *</label>
              <Input
                required
                type="number"
                min="1000"
                value={salePrice}
                onChange={(e) => setSalePrice(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-sm font-mono text-rose-400 font-bold"
              />
            </div>
          </div>

          {/* Quota Type Selection */}
          <div className="space-y-2 pt-2">
            <label className="text-slate-300 font-semibold block">Hạn mức người dùng *</label>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              <button
                type="button"
                onClick={() => setQuotaType("SINGLE")}
                className={`p-3 rounded-2xl border text-left space-y-1 transition-all ${
                  quotaType === "SINGLE"
                    ? "bg-rose-600/20 border-rose-500 text-rose-300 shadow-md shadow-rose-950"
                    : "bg-slate-800/60 border-slate-700 text-slate-400 hover:text-slate-200"
                }`}
              >
                <div className="font-black text-xs flex items-center gap-1.5">
                  <ShieldCheck className="w-4 h-4 text-rose-400" />
                  Loại 1: Deal Sốc (1 suất)
                </div>
                <p className="text-[10px] text-slate-400">Mỗi khách chỉ mua được 1 lần / 1 suất.</p>
              </button>

              <button
                type="button"
                onClick={() => setQuotaType("MULTIPLE")}
                className={`p-3 rounded-2xl border text-left space-y-1 transition-all ${
                  quotaType === "MULTIPLE"
                    ? "bg-emerald-600/20 border-emerald-500 text-emerald-300 shadow-md shadow-emerald-950"
                    : "bg-slate-800/60 border-slate-700 text-slate-400 hover:text-slate-200"
                }`}
              >
                <div className="font-black text-xs flex items-center gap-1.5">
                  <Users className="w-4 h-4 text-emerald-400" />
                  Loại 2: Xả Kho (Nhiều suất)
                </div>
                <p className="text-[10px] text-slate-400">Khách được mua tích lũy qua nhiều đơn.</p>
              </button>
            </div>

            {quotaType === "MULTIPLE" && (
              <div className="p-3 bg-slate-800/80 rounded-xl border border-slate-700 space-y-1 animate-in fade-in">
                <label className="text-slate-300 font-semibold block">Hạn mức tối đa/khách:</label>
                <Input
                  type="number"
                  min="2"
                  max="100"
                  value={maxUser}
                  onChange={(e) => setMaxUser(e.target.value)}
                  className="bg-slate-900 border-slate-600 text-white h-9 font-mono"
                />
              </div>
            )}
          </div>

          <div className="flex gap-3 pt-3">
            <Button
              type="submit"
              disabled={submitting}
              className="flex-1 bg-gradient-to-r from-amber-500 to-rose-600 hover:from-amber-600 hover:to-rose-700 text-white font-black text-xs h-11 rounded-xl shadow-lg shadow-rose-600/20"
            >
              {submitting ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin mr-2" />
                  Đang cập nhật...
                </>
              ) : (
                "CẬP NHẬT SẢN PHẨM"
              )}
            </Button>
            <Button
              type="button"
              variant="outline"
              onClick={onClose}
              className="border-slate-700 text-slate-300 hover:bg-slate-800 h-11 rounded-xl"
            >
              Hủy
            </Button>
          </div>
        </form>
      </div>
    </div>
  );
}
