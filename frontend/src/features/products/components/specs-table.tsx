import { Cpu } from "lucide-react";

interface SpecsTableProps {
  specifications?: Record<string, any>;
}

// Từ điển dịch thông số kỹ thuật điện máy sang tiếng Việt
const SPEC_LABELS: Record<string, string> = {
  btu: "Công suất làm lạnh (BTU)",
  inverter: "Công nghệ tiết kiệm điện Inverter",
  power_hp: "Công suất ngựa (HP)",
  gas_type: "Loại Gas làm lạnh",
  air_purification: "Công nghệ lọc khí",
  capacity_liters: "Dung tích thực (Lít)",
  door_type: "Kiểu tủ lạnh",
  cooling_technology: "Công nghệ làm lạnh",
  dimensions_mm: "Kích thước (Rộng x Cao x Sâu mm)",
  total_power_w: "Tổng công suất (W)",
  cooking_zones: "Số vùng nấu",
  glass_type: "Chất liệu mặt kính",
  booster: "Gia nhiệt nhanh PowerBoost",
  auto_cut_off: "Tự động ngắt an toàn",
  screen_size_inch: "Kích thước màn hình",
  resolution: "Độ phân giải",
  display_tech: "Công nghệ hình ảnh",
  operating_system: "Hệ điều hành",
  refresh_rate_hz: "Tần số quét (Hz)",
  warranty_months: "Thời hạn bảo hành chính hãng",
  compressor_warranty_years: "Bảo hành máy nén",
};

export function SpecsTable({ specifications }: SpecsTableProps) {
  if (!specifications || Object.keys(specifications).length === 0) {
    return (
      <div className="p-6 text-center text-slate-400 bg-slate-50 dark:bg-slate-800/40 rounded-2xl border border-slate-100 dark:border-slate-800">
        Đang cập nhật thông số kỹ thuật chi tiết
      </div>
    );
  }

  return (
    <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-100 dark:border-slate-800 overflow-hidden shadow-sm">
      <div className="p-4 border-b border-slate-100 dark:border-slate-800 bg-slate-50/70 dark:bg-slate-800/50 flex items-center gap-2">
        <Cpu className="w-5 h-5 text-blue-600" />
        <h3 className="font-bold text-slate-900 dark:text-white text-base">
          Bảng Thông Số Kỹ Thuật Chi Tiết
        </h3>
      </div>

      <div className="divide-y divide-slate-100 dark:divide-slate-800">
        {Object.entries(specifications).map(([key, value], idx) => {
          const label = SPEC_LABELS[key] || key;
          let displayValue = String(value);

          if (typeof value === "boolean") {
            displayValue = value ? "Có (Tiết kiệm điện cao cấp)" : "Không";
          } else if (key === "warranty_months") {
            displayValue = `${value} Tháng`;
          } else if (key === "compressor_warranty_years") {
            displayValue = `${value} Năm`;
          } else if (key === "capacity_liters") {
            displayValue = `${value} Lít`;
          } else if (key === "screen_size_inch") {
            displayValue = `${value} Inch`;
          } else if (key === "total_power_w") {
            displayValue = `${value} W`;
          }

          return (
            <div
              key={key}
              className={`flex flex-col sm:flex-row sm:items-center justify-between p-3.5 text-sm ${
                idx % 2 === 0 ? "bg-white dark:bg-slate-900" : "bg-slate-50/50 dark:bg-slate-800/30"
              }`}
            >
              <span className="font-medium text-slate-500 dark:text-slate-400 sm:w-1/2">
                {label}
              </span>
              <span className="font-semibold text-slate-900 dark:text-slate-100 sm:w-1/2 sm:text-right mt-1 sm:mt-0">
                {displayValue}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
