"""
Gerador de eventos de pedidos da NovaMarket -> Kafka (tópico "orders").

- Chave da mensagem: customer_id (garante ordem dos eventos do mesmo cliente
  dentro de uma partição e distribui bem a carga entre as 3 partições).
- ~5% dos eventos são DUPLICADOS (mesmo event_id) -> tratados na camada TRUSTED.
- ~5% dos eventos chegam ATRASADOS (event_time 2 a 4 min no passado).
- Modo --late-demo: envia 2 eventos atrasados "de propósito" para a demonstração
  do watermark (um dentro da tolerância e outro fora dela).

Uso (dentro do container):
    python /opt/project/producer/producer.py --events 2000 --rate 50
    python /opt/project/producer/producer.py --late-demo
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

TOPIC = "orders"

PRODUCTS = {  # product_id: (categoria, preço base)
    "P001": ("eletronicos", 1899.90), "P002": ("eletronicos", 349.90),
    "P003": ("moda", 129.90), "P004": ("moda", 89.90),
    "P005": ("casa", 259.90), "P006": ("casa", 59.90),
    "P007": ("beleza", 79.90), "P008": ("beleza", 39.90),
    "P009": ("esportes", 199.90), "P010": ("esportes", 149.90),
}
CAMPAIGNS = ["black_friday", "frete_gratis", "cupom10", "nenhuma"]


def build_event(event_time=None):
    product_id = random.choice(list(PRODUCTS))
    category, price = PRODUCTS[product_id]
    now = datetime.now(timezone.utc)
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"O{random.randint(100000, 999999)}",
        "customer_id": f"C{random.randint(1, 500):04d}",
        "product_id": product_id,
        "category": category,
        "campaign": random.choice(CAMPAIGNS),
        "quantity": random.randint(1, 3),
        "price": round(price * random.uniform(0.9, 1.0), 2),
        "event_time": (event_time or now).isoformat(),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--events", type=int, default=2000)
    p.add_argument("--rate", type=int, default=50, help="eventos por segundo")
    p.add_argument("--late-demo", action="store_true")
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",       # só confirma quando o broker gravou
        linger_ms=10,     # agrupa mensagens em lotes (melhor throughput)
    )

    if args.late_demo:
        now = datetime.now(timezone.utc)
        for minutes, label in [(3, "ATRASADO (dentro do watermark)"),
                               (20, "MUITO ATRASADO (fora do watermark)")]:
            ev = build_event(now - timedelta(minutes=minutes))
            ev["category"] = "evento_atrasado"
            producer.send(TOPIC, key=ev["customer_id"], value=ev)
            print(f"[late-demo] {label}: event_time={ev['event_time']}")
        producer.flush()
        return

    sent = dup = late = 0
    last = None
    for _ in range(args.events):
        ev = build_event()
        r = random.random()
        if r < 0.05 and last:                 # duplicado
            ev = last
            dup += 1
        elif r < 0.10:                        # atrasado
            ev["event_time"] = (datetime.now(timezone.utc)
                                - timedelta(minutes=random.randint(2, 4))).isoformat()
            late += 1
        producer.send(TOPIC, key=ev["customer_id"], value=ev)
        last = ev
        sent += 1
        if sent % 500 == 0:
            print(f"enviados={sent} duplicados={dup} atrasados={late}")
        time.sleep(1 / args.rate)

    producer.flush()
    print(f"FIM: enviados={sent} duplicados={dup} atrasados={late}")


if __name__ == "__main__":
    main()
