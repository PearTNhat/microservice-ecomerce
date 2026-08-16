Dưới đây là file mẫu **`PRODUCTION_STRUCTURE.md`** thiết kế riêng cho **Antigravity** (hoặc các AI coding agent như Cursor, Claude), tuân thủ tuyệt đối chuẩn **Next.js App Router** tối ưu production (tránh waterfall, tối ưu bundle, tách biệt Server/Client component).

Bạn có thể copy nội dung này tạo thành file `.cursorrules`, `AGENTS.md` hoặc đưa trực tiếp cho Antigravity đọc.

---

```markdown
# 🚀 Next.js Production Architecture & Coding Standards

## 📁 1. Cấu trúc thư mục chuẩn Production (App Router)

```text
src/
├── app/                       # App Router (Routing & Layouts)
│   ├── (auth)                 # Route Groups (gom nhóm UI không ảnh hưởng URL)
│   │   ├── login/
│   │   │   └── page.tsx
│   │   └── register/
│   │       └── page.tsx
│   ├── dashboard/             # Route thực tế: /dashboard
│   │   ├── components/        # Component chỉ dùng riêng cho dashboard route này
│   │   ├── loading.tsx        # UI Skeleton/Loading tiêu chuẩn
│   │   ├── error.tsx          # Error Boundary xử lý lỗi phân đoạn
│   │   └── page.tsx
│   ├── layout.tsx             # Root Layout chung toàn app
│   ├── page.tsx               # Homepage (/)
│   └── globals.css            # Tailwind CSS & Global styles
│
├── components/                # Thành phần UI dùng chung toàn hệ thống
│   ├── ui/                    # Atomic components (Button, Input, Dialog, Dropdown...)
│   └── layouts/               # Header, Footer, Sidebar, Navigation...
│
├── features/                  # Feature-based Architecture (chia theo tính năng lớn)
│   └── products/
│       ├── components/        # ProductCard, ProductFilter...
│       ├── services/          # API calls / Server Actions riêng cho product
│       └── types/             # TypeScript interfaces/types của sản phẩm
│
├── lib/                       # Khởi tạo thư viện bên thứ 3 (prisma, axios client, auth config)
├── services/                  # Global API services / Repository pattern
├── hooks/                     # Custom React Hooks dùng chung (useDebounce, useMediaQuery...)
├── store/                     # State Management (Zustand, TanStack Query...)
└── types/                     # TypeScript types/interfaces chung toàn cục

```

---

## ⚡ 2. Các quy tắc lập trình bắt buộc (Strict Coding Rules)

### A. Server vs Client Components

1. **Mặc định là Server Component:** Tất cả các component trong `app/` mặc định là Server Component trừ khi có yêu cầu tương tác.
2. **"use client" đúng chỗ:** Chỉ thêm `"use client"` ở dòng đầu tiên của file khi bắt buộc sử dụng:
* React Hooks (`useState`, `useEffect`, `useContext`, `useRef`...)
* Trình duyệt APIs (`window`, `localStorage`, `navigator`...)
* DOM Events (`onClick`, `onChange`, `onSubmit`...)



### B. Hiệu năng & Chống Waterfalls (Vercel Standards)

1. **Loại bỏ Waterfalls:** Luôn dùng `Promise.all()` cho các lệnh fetch dữ liệu độc lập nhau.
2. **Tránh Barrel Files (Index files gộp):** Không dùng `import { Button, Input } from '@/components'`. Phải import tường minh từng file: `import { Button } from '@/components/ui/button'`.
3. **Caching & Revalidation:** Tận dụng `fetch(url, { next: { revalidate: 3600 } })` hoặc `React.cache()` để khử trùng lặp request trong cùng vòng đời request.

### C. Giao diện & UX Guidelines

1. **Trạng thái Loading:** Các nút Submit/Action **bắt buộc** phải có hiệu ứng loading/spinner và disable khi đang xử lý để tránh user click đúp.
2. **Mobile Input Size:** Kích thước chữ (`font-size`) của mọi trường input trên mobile phải $\ge$ 16px để Safari trên iOS không tự động zoom màn hình.
3. **Import Alias:** Luôn sử dụng đường dẫn tuyệt đối với alias `@/*` (tránh tuyệt đối dùng `../../../components/...`).

```

```