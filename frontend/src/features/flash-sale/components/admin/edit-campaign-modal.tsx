"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { flashSaleService } from "@/features/flash-sale/services/flash-sale-service";
import { AdminCampaign } from "@/features/flash-sale/types";
import { Loader2, Pencil, X } from "lucide-react";
import React, { useEffect, useState } from "react";

interface EditCampaignModalProps {
  campaign: AdminCampaign | null;
  onClose: () => void;
  onSuccess: (message: string) => void;
  onError: (error: string) => void;
}

const toISOInput = (d: Date | string) => {
  const dt = new Date(d);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${dt.getFullYear()}-${pad(dt.getMonth() + 1)}-${pad(dt.getDate())}T${pad(dt.getHours())}:${pad(dt.getMinutes())}`;
};

export function EditCampaignModal({
  campaign,
  onClose,
  onSuccess,
  onError,
}: EditCampaignModalProps) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [startsAt, setStartsAt] = useState("");
  const [endsAt, setEndsAt] = useState("");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (campaign) {
      setName(campaign.name);
      setDescription(campaign.description || "");
      setStartsAt(toISOInput(campaign.starts_at));
      setEndsAt(toISOInput(campaign.ends_at));
    }
  }, [campaign]);

  if (!campaign) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name || !startsAt || !endsAt) {
      alert("Vui lòng điền đầy đủ tên và thời gian chiến dịch");
      return;
    }

    try {
      setSubmitting(true);
      await flashSaleService.updateCampaign(campaign.id, {
        name,
        description,
        starts_at: new Date(startsAt).toISOString(),
        ends_at: new Date(endsAt).toISOString(),
      });
      onSuccess(`Cập nhật chiến dịch #${campaign.id} thành công!`);
      onClose();
    } catch (err: any) {
      onError(err?.message || "Cập nhật chiến dịch thất bại");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-200">
      <div className="relative w-full max-w-lg bg-slate-900 border border-blue-500/30 rounded-3xl shadow-2xl overflow-hidden text-slate-100 flex flex-col max-h-[90vh]">
        <div className="p-5 bg-gradient-to-r from-blue-600 to-indigo-600 text-white flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <div className="p-2 bg-white/20 rounded-xl">
              <Pencil className="w-5 h-5 text-white" />
            </div>
            <div>
              <h3 className="text-lg font-black tracking-tight">Sửa Chiến Dịch #{campaign.id}</h3>
              <p className="text-xs text-blue-100">Thay đổi tên, mô tả và khung thời gian mở bán</p>
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
          <div>
            <label className="text-slate-300 font-semibold block mb-1">Tên chiến dịch *</label>
            <Input
              required
              placeholder="Tên chiến dịch"
              value={name}
              onChange={(e) => setName(e.target.value)}
              className="bg-slate-800 border-slate-700 text-white h-10 text-sm"
            />
          </div>

          <div>
            <label className="text-slate-300 font-semibold block mb-1">Mô tả ngắn</label>
            <Input
              placeholder="Mô tả chiến dịch"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              className="bg-slate-800 border-slate-700 text-white h-10 text-sm"
            />
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Thời gian bắt đầu *</label>
              <Input
                required
                type="datetime-local"
                value={startsAt}
                onChange={(e) => setStartsAt(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-xs font-mono"
              />
            </div>
            <div>
              <label className="text-slate-300 font-semibold block mb-1">Thời gian kết thúc *</label>
              <Input
                required
                type="datetime-local"
                value={endsAt}
                onChange={(e) => setEndsAt(e.target.value)}
                className="bg-slate-800 border-slate-700 text-white h-10 text-xs font-mono"
              />
            </div>
          </div>

          <div className="flex gap-3 pt-3">
            <Button
              type="submit"
              disabled={submitting}
              className="flex-1 bg-blue-600 hover:bg-blue-700 text-white font-black text-xs h-11 rounded-xl shadow-lg shadow-blue-600/20"
            >
              {submitting ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin mr-2" />
                  Đang lưu thay đổi...
                </>
              ) : (
                "LƯU THÔNG TIN CHIẾN DỊCH"
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
