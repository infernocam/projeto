import asyncio
import json
import websockets
import time
from datetime import datetime

print("🚀 SERVIDOR REPDESK RENDER - MODO HÍBRIDO SINALIZADOR")
print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)

# Estruturas para gerenciar conexões e sessões ativas
conexoes = {}  # { "id_limpo": websocket }
salas = {}     # { "id_origem": "id_destino" } para mapear quem está conectado com quem

def limpar_id(id_bruto):
    """Remove hífens e espaços para evitar erros de digitação"""
    return str(id_bruto).replace("-", "").strip() if id_bruto else None

async def handler(websocket, path):
    cliente_id_original = None
    cliente_id_limpo = None
    
    try:
        async for mensagem in websocket:
            dados = json.loads(mensagem)
            tipo = dados.get("tipo")
            
            # 1. REGISTRO INICIAL DA MÁQUINA
            if tipo == "registrar":
                cliente_id_original = dados.get("id")
                cliente_id_limpo = limpar_id(cliente_id_original)
                
                conexoes[cliente_id_limpo] = websocket
                
                print(f"✅ Dispositivo Conectado: {cliente_id_original} (Limpo: {cliente_id_limpo})")
                print(f"📊 Dispositivos online: {len(conexoes)}")
                
                await websocket.send(json.dumps({
                    "tipo": "registro_ok",
                    "mensagem": "Conectado com sucesso ao barramento REPDESK"
                }))
            
            # 2. SOLICITAÇÃO DE ACESSO (ENCAMINHAMENTO)
            elif tipo == "pedir_permissao":
                alvo_id = limpar_id(dados.get("alvo_id"))
                tecnico_id = dados.get("tecnico_id")
                
                print(f"🔍 {tecnico_id} está solicitando controle da máquina {dados.get('alvo_id')}")
                
                if alvo_id in conexoes:
                    await conexoes[alvo_id].send(json.dumps({
                        "tipo": "pedido_permissao",
                        "tecnico_id": tecnico_id
                    }))
                    print(f"📨 Solicitação entregue para {dados.get('alvo_id')}")
                else:
                    await websocket.send(json.dumps({
                        "tipo": "permissao_resultado",
                        "aceito": False,
                        "mensagem": "Esta estação de trabalho não foi encontrada ou está offline."
                    }))
                    print(f"❌ Alvo {dados.get('alvo_id')} não está online.")
            
            # 3. RESPOSTA DO MODAL DE SEGURANÇA (AUTORIZAR / RECUSAR)
            elif tipo == "resposta_permissao":
                tecnico_id_limpo = limpar_id(dados.get("tecnico_id"))
                alvo_id_original = dados.get("alvo_id")
                alvo_id_limpo = limpar_id(alvo_id_original)
                aceito = dados.get("aceito")
                
                print(f"📣 Resposta do Alvo {alvo_id_original}: {'AUTORIZADO' if aceito else 'RECUSADO'}")
                
                if tecnico_id_limpo in conexoes:
                    await conexoes[tecnico_id_limpo].send(json.dumps({
                        "tipo": "permissao_resultado",
                        "aceito": aceito,
                        "alvo_id": alvo_id_original
                    }))
                    
                    if aceito:
                        salas[tecnico_id_limpo] = alvo_id_limpo
                        salas[alvo_id_limpo] = tecnico_id_limpo
                        print(f"🔗 SESSÃO ESTABELECIDA: {tecnico_id_limpo} <-> {alvo_id_limpo}")
            
            # 4. TRÁFEGO DE STREAMING DE VÍDEO
            elif tipo == "webrtc_offer":
                alvo_id_limpo = limpar_id(dados.get("alvo_id"))
                if alvo_id_limpo in conexoes:
                    await conexoes[alvo_id_limpo].send(json.dumps({
                        "tipo": "webrtc_offer",
                        "frame": dados.get("frame")
                    }))
            
            # 5. TRÁFEGO DE COMANDOS
            elif tipo == "input":
                alvo_id_limpo = limpar_id(dados.get("alvo_id"))
                if alvo_id_limpo in conexoes:
                    await conexoes[alvo_id_limpo].send(json.dumps(dados))
            
            # 6. ENCERRAMENTO DE SESSÃO
            elif tipo == "fechar_sessao":
                parceiro_limpo = salas.get(cliente_id_limpo)
                if parceiro_limpo and parceiro_limpo in conexoes:
                    await conexoes[parceiro_limpo].send(json.dumps({"tipo": "sessao_encerrada"}))
                    if parceiro_limpo in salas: del salas[parceiro_limpo]
                if cliente_id_limpo in salas: del salas[cliente_id_limpo]
                print(f"⏹ Sessão encerrada por iniciativa de {cliente_id_original}")

            # ==========================================
            # 7. NOVA OTIMIZAÇÃO: KEEP-ALIVE (PING/PONG)
            # ==========================================
            elif tipo == "ping":
                # O servidor recebe um "ping" e devolve um "pong" silencioso
                await websocket.send(json.dumps({"tipo": "pong"}))
                
    except websockets.exceptions.ConnectionClosed:
        print(f"🔌 Conexão de rede perdida abruptamente: {cliente_id_original}")
    except Exception as e:
        print(f"❌ Erro operacional no processamento do Handler: {e}")
    finally:
        if cliente_id_limpo and cliente_id_limpo in conexoes:
            del conexoes[cliente_id_limpo]
            parceiro_limpo = salas.get(cliente_id_limpo)
            if parceiro_limpo and parceiro_limpo in conexoes:
                try:
                    await conexoes[parceiro_limpo].send(json.dumps({"tipo": "sessao_encerrada"}))
                except: pass
                if parceiro_limpo in salas: del salas[parceiro_limpo]
            if cliente_id_limpo in salas: del salas[cliente_id_limpo]
            
            print(f"👋 Removido do barramento: {cliente_id_original}")
            print(f"📊 Dispositivos online restantes: {len(conexoes)}")

async def main():
    async with websockets.serve(handler, "0.0.0.0", 10000, ping_interval=None, max_size=10*1024*1024):
        print("✅ SERVIDOR OTIMIZADO COM KEEP-ALIVE PRONTO!")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
