import asyncio
import json
import websockets
import time
from datetime import datetime

print("🚀 SERVIDOR REPDESK RENDER")
print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*50)

conexoes = {}
salas = {}

async def handler(websocket, path):
    cliente_id = None
    
    try:
        async for mensagem in websocket:
            dados = json.loads(mensagem)
            tipo = dados.get("tipo")
            
            if tipo == "registrar":
                cliente_id = dados.get("id")
                tipo_cliente = dados.get("tipo_cliente")
                conexoes[cliente_id] = {
                    "ws": websocket,
                    "tipo": tipo_cliente,
                    "conectado_em": time.time()
                }
                print(f"✅ Registrado: {cliente_id} ({tipo_cliente})")
                print(f"📊 Total conexões: {len(conexoes)}")
                
                await websocket.send(json.dumps({
                    "tipo": "registro_ok",
                    "mensagem": "Conectado ao servidor REPDESK"
                }))
            
            elif tipo == "cliente_conectar":
                servidor_id = dados.get("servidor_id")
                cliente_id_req = dados.get("cliente_id")
                
                print(f"🔍 Cliente {cliente_id_req} procurando servidor {servidor_id}")
                
                if servidor_id in conexoes and conexoes[servidor_id]["tipo"] == "servidor":
                    servidor_ws = conexoes[servidor_id]["ws"]
                    
                    await servidor_ws.send(json.dumps({
                        "tipo": "novo_cliente",
                        "cliente_id": cliente_id_req,
                        "servidor_id": servidor_id
                    }))
                    print(f"📨 Notificação enviada ao servidor {servidor_id}")
                else:
                    await websocket.send(json.dumps({
                        "tipo": "erro",
                        "msg": f"Servidor {servidor_id} não está online"
                    }))
                    print(f"❌ Servidor {servidor_id} não encontrado")
            
            elif tipo == "resposta_conexao":
                cliente_id_resp = dados.get("cliente_id")
                servidor_id = dados.get("servidor_id")
                aceito = dados.get("aceito")
                
                print(f"📨 Resposta do servidor {servidor_id} para cliente {cliente_id_resp}: {'ACEITO' if aceito else 'RECUSADO'}")
                
                if cliente_id_resp in conexoes:
                    cliente_ws = conexoes[cliente_id_resp]["ws"]
                    
                    await cliente_ws.send(json.dumps({
                        "tipo": "conexao_aceita" if aceito else "conexao_recusada",
                        "servidor_id": servidor_id,
                        "cliente_id": cliente_id_resp
                    }))
                    
                    if aceito:
                        salas[cliente_id_resp] = servidor_id
                        salas[servidor_id] = cliente_id_resp
                        print(f"🔗 CONEXÃO ESTABELECIDA: {servidor_id} <-> {cliente_id_resp}")
            
            elif tipo == "comando":
                destino_id = dados.get("destino_id")
                origem_id = dados.get("origem_id")
                comando = dados.get("comando")
                
                if destino_id in conexoes:
                    await conexoes[destino_id]["ws"].send(json.dumps({
                        "tipo": "comando_recebido",
                        "comando": comando,
                        "origem_id": origem_id,
                        "timestamp": time.time()
                    }))
                    print(f"📡 Comando encaminhado: {comando[:50]}...")
                else:
                    print(f"❌ Destino {destino_id} não encontrado")
            
            elif tipo == "resultado_comando":
                destino_id = dados.get("destino_id")
                resultado = dados.get("resultado")
                
                if destino_id in conexoes:
                    await conexoes[destino_id]["ws"].send(json.dumps({
                        "tipo": "resultado_comando",
                        "resultado": resultado
                    }))
                    print(f"✅ Resultado enviado para {destino_id}")
            
            elif tipo == "frame":
                destino_id = dados.get("destino_id")
                
                if destino_id in conexoes:
                    await conexoes[destino_id]["ws"].send(json.dumps({
                        "tipo": "frame",
                        "frame": dados.get("frame"),
                        "qualidade": dados.get("qualidade", 85)
                    }))
                    # print(f"📸 Frame enviado para {destino_id}")
            
            elif tipo == "ping":
                await websocket.send(json.dumps({"tipo": "pong"}))
                
    except websockets.exceptions.ConnectionClosed:
        print(f"🔌 Conexão fechada: {cliente_id}")
    except Exception as e:
        print(f"❌ Erro no handler: {e}")
    finally:
        if cliente_id and cliente_id in conexoes:
            del conexoes[cliente_id]
            print(f"👋 Desconectado: {cliente_id}")
            print(f"📊 Conexões restantes: {len(conexoes)}")

async def main():
    async with websockets.serve(handler, "0.0.0.0", 10000):
        print("✅ SERVIDOR RODANDO!")
        print("🌍 URL: wss://projeto-43j6.onrender.com")
        print("📡 Porta interna: 10000")
        print("="*50)
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())