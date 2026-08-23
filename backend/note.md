Hôm trước có đi phỏng vấn được hỏi đến vấn đề transaction trong microservice.
Bài toán đặt ra cho em là service A thực hiện xong rồi sẽ call sang service B xử lý tiếp, sau đó lại trả về cho service A xử lý nốt phần còn lại.
Bên pv có hỏi em
Em xử lý transaction ở đây như thế nào nếu service B xử lý lỗi
và tiếp đó xử lý như nào nếu service B xong rồi trả về service A nhưng đến lúc đó service A mới lỗi.
Mong mng có thể chỉ cho em hướng tiếp cận với bài toán để em có thể tìm hiểu thêm.
Em cũng muốn hỏi thêm là về SQL thì e nên tìm hiểu sâu hơn về phần nào?

Tìm hiểu distributed transaction nhé, tiêu biểu có saga, 2pc, nên xem ưu nhược điểm mỗi loại