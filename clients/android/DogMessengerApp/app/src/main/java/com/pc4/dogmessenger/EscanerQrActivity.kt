package com.pc4.dogmessenger

import android.app.Activity
import android.content.Intent
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.activity.compose.rememberLauncherForActivityResult
import com.journeyapps.barcodescanner.ScanContract
import com.journeyapps.barcodescanner.ScanOptions
import com.pc4.dogmessenger.ui.theme.DogMessengerAppTheme

class EscanerQrActivity : ComponentActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        // Este lanzador abre la cámara y espera el resultado del QR
        val scanLauncher = registerForActivityResult(ScanContract()) { result ->
            if (result.contents != null) {
                // Devolvemos el texto leído (ej. "192.168.1.45:9105:TOKEN123") a MainActivity
                val intent = Intent()
                intent.putExtra("qr_data", result.contents)
                setResult(Activity.RESULT_OK, intent)
                finish()
            }
        }

        setContent {
            DogMessengerAppTheme {
                Column(
                    modifier = Modifier.fillMaxSize(),
                    verticalArrangement = Arrangement.Center,
                    horizontalAlignment = Alignment.CenterHorizontally
                ) {
                    Text("Vinculación de Dispositivo", style = MaterialTheme.typography.headlineMedium)
                    Spacer(modifier = Modifier.height(20.dp))
                    Text("Escanea el código QR que aparece en la PC")
                    Spacer(modifier = Modifier.height(30.dp))
                    Button(onClick = {
                        val options = ScanOptions()
                        options.setDesiredBarcodeFormats(ScanOptions.QR_CODE)
                        options.setPrompt("Escanea el QR del servidor")
                        options.setCameraId(0) // Cámara trasera
                        options.setBeepEnabled(false)
                        options.setBarcodeImageEnabled(true)
                        scanLauncher.launch(options)
                    }) {
                        Text("Abrir Cámara")
                    }
                }
            }
        }
    }
}