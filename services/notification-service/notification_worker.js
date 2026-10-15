const amqp = require('amqplib');

const AMQP_URI = 'amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/medcore_vhost';
const EXCHANGE_NAME = 'medcore.clinical.events';
const QUEUE_NAME = 'notification.patient.alert.q';
const ROUTING_KEY_PATTERN = 'clinical.#'; // Mendengarkan seluruh event klinis

async function startNotificationWorker() {
    try {
        // 1. Koneksi ke RabbitMQ Broker
        const connection = await amqp.connect(AMQP_URI);
        const channel = await connection.createChannel();

        // 2. Deklarasikan Exchange dan Queue yang durable
        await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });
        const q = await channel.assertQueue(QUEUE_NAME, { durable: true });

        // 3. Bind Queue ke Exchange dengan Routing Key Pattern
        await channel.bindQueue(q.queue, EXCHANGE_NAME, ROUTING_KEY_PATTERN);

        // 4. Batasi prefetch count agar pesan diproses satu per satu secara adil
        await channel.prefetch(1);
        console.log(`[Notification Worker] Berjalan pada antrean: ${q.queue}`);

        // 5. Konsumsi pesan secara asinkron dengan manual ACK
        channel.consume(q.queue, (msg) => {
            if (msg !== null) {
                const eventData = JSON.parse(msg.content.toString());
                
                console.log("\n--------------------------------------------------");
                console.log(`[SMS/WhatsApp Gateway] Mengirim Notifikasi ke Pasien: ${eventData.payload.patient_id}`);
                console.log(`Pesan: "Sesi konsultasi Anda (${eventData.payload.appointment_id}) telah selesai. Resep sedang disiapkan oleh Farmasi."`);
                
                // Konfirmasi manual bahwa pesan sukses diproses
                channel.ack(msg);
                console.log(`[Notification Worker] Event ${eventData.event_id} di-ACK.`);
                console.log("--------------------------------------------------");
            }
        }, { noAck: false });

    } catch (error) {
        console.error('[Notification Worker] Error:', error);
    }
}

startNotificationWorker();