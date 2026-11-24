package com.pc4.dogmessenger

import android.app.Activity
import android.content.Intent
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.launch
import com.pc4.dogmessenger.ui.theme.DogMessengerAppTheme

class MainActivity : ComponentActivity() {

    private lateinit var networkClient: NetworkClient
    private var ipServidor by mutableStateOf("")
    private var puertoServidor by mutableStateOf(0)
    private var claveServidor by mutableStateOf("")

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setContent {
            DogMessengerAppTheme {
                // Estado para saber si ya escaneamos el QR y estamos conectados
                var conexionLista by remember { mutableStateOf(false) }

                // Lanzador para abrir la pantalla de escaneo
                val qrLauncher = rememberLauncherForActivityResult(
                    contract = ActivityResultContracts.StartActivityForResult()
                ) { result ->
                    if (result.resultCode == Activity.RESULT_OK) {
                        val qrData = result.data?.getStringExtra("qr_data") ?: ""
                        // El QR tiene el formato: "IP:PUERTO:CLAVE" (ej. "192.168.1.45:9105:MI_CLAVE")
                        val partes = qrData.split(":")
                        if (partes.size >= 2) {
                            ipServidor = partes[0]
                            puertoServidor = partes[1].toIntOrNull() ?: 9105

                            // Si el QR trae una clave (3ra parte), la usamos. Si no, una por defecto.
                            claveServidor = if (partes.size >= 3) partes[2] else "CLAVE_POR_DEFECTO"

                            // Nos conectamos automáticamente pasándole la clave para el cifrado
                            networkClient = NetworkClient(ipServidor, puertoServidor, claveServidor)
                            lifecycleScope.launch {
                                val ok = networkClient.conectar()
                                if (ok) conexionLista = true
                            }
                        }
                    }
                }

                if (!conexionLista) {
                    // PANTALLA 1: Pide escanear el QR
                    Column(
                        modifier = Modifier.fillMaxSize(),
                        verticalArrangement = Arrangement.Center,
                        horizontalAlignment = Alignment.CenterHorizontally
                    ) {
                        Text("Dog Messenger", style = MaterialTheme.typography.headlineLarge)
                        Spacer(modifier = Modifier.height(20.dp))
                        Text("Para empezar, escanea el QR de la PC")
                        Spacer(modifier = Modifier.height(30.dp))
                        Button(onClick = {
                            val intent = Intent(this@MainActivity, EscanerQrActivity::class.java)
                            qrLauncher.launch(intent)
                        }) {
                            Text("Escanear QR")
                        }
                    }
                } else {
                    // PANTALLA 2: El Chat (ya conectados)
                    ChatScreen(
                        serverIp = ipServidor,
                        onSendMessage = { mensaje ->
                            lifecycleScope.launch {
                                // Enviamos el mensaje (NetworkClient lo cifrará antes de salir)
                                networkClient.enviarMensaje(mensaje, "app_android")
                            }
                        }
                    )
                }
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ChatScreen(serverIp: String, onSendMessage: (String) -> Unit) {
    var texto by remember { mutableStateOf("") }
    val mensajes = remember { mutableStateListOf<String>() }

    Scaffold(
        topBar = { TopAppBar(title = { Text("Conectado a $serverIp") }) }
    ) { padding ->
        Column(modifier = Modifier.padding(padding).fillMaxSize()) {

            // Lista de mensajes
            LazyColumn(modifier = Modifier.weight(1f).padding(16.dp)) {
                items(mensajes) { msg ->
                    Text(text = msg, modifier = Modifier.padding(4.dp))
                }
            }

            // Caja de texto y botón enviar
            Row(modifier = Modifier.fillMaxWidth().padding(8.dp)) {
                OutlinedTextField(
                    value = texto,
                    onValueChange = { texto = it },
                    modifier = Modifier.weight(1f),
                    placeholder = { Text("Escribe un mensaje...") }
                )
                Spacer(modifier = Modifier.width(8.dp))
                Button(onClick = {
                    if (texto.isNotBlank()) {
                        mensajes.add("Yo: $texto")
                        onSendMessage(texto)
                        texto = ""
                    }
                }) {
                    Text("Enviar")
                }
            }
        }
    }
}