package com.pc4.dogmessenger

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.launch
import com.pc4.dogmessenger.ui.theme.DogMessengerAppTheme

class MainActivity : ComponentActivity() {

    // ⚠️ AQUÍ DEBES PONER LA IP LOCAL DE TU PC (donde corre el nodo Kotlin)
    private val serverIp = "172.16.64.51" // CAMBIAR POR TU IP REAL
    private val serverPort = 9105          // El puerto de tu nodo Kotlin (n3)

    private lateinit var networkClient: NetworkClient

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        networkClient = NetworkClient(serverIp, serverPort)

        // Nos conectamos en segundo plano al iniciar la app
        lifecycleScope.launch {
            val conectado = networkClient.conectar()
            if (!conectado) {
                // Aquí podrías mostrar un Toast de error
            }
        }

        setContent {
            DogMessengerAppTheme {
                ChatScreen(onSendMessage = { mensaje ->
                    lifecycleScope.launch {
                        networkClient.enviarMensaje(mensaje, "app_android")
                    }
                })
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ChatScreen(onSendMessage: (String) -> Unit) {
    var texto by remember { mutableStateOf("") }
    val mensajes = remember { mutableStateListOf<String>() }

    Scaffold(
        topBar = { TopAppBar(title = { Text("Dog Messenger") }) }
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