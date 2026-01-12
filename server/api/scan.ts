import { exec } from 'node:child_process'
import { promisify } from 'node:util'
import { readFile, readdir } from 'node:fs/promises'
import { join } from 'node:path'

const execAsync = promisify(exec)

export default defineEventHandler(async (event) => {
  try {
    // 1. Définition des chemins
    const scriptPath = join(process.cwd(), 'scripts', 'flip_extra_flips_live.py')
    const outDir = join(process.cwd(), 'scripts', 'out')

    // 2. Commande Python
    // Note: Assurez-vous que 'python3' est bien dans le PATH, sinon utilisez le chemin absolu
    const command = `python3 "${scriptPath}" --epoch 0 --threshold 3 --out-dir "${outDir}"`
    
    console.log('--- START SCAN ---')
    console.log('Script:', scriptPath)
    console.log('Output Dir:', outDir)

    // Exécution
    await execAsync(command)

    // 3. Lecture du résultat
    const files = await readdir(outDir)
    
    // CORRECTION ICI : on précise que 'f' est une string
    const jsonFile = files.find((f: string) => f.endsWith('.meta.json'))

    if (!jsonFile) {
      throw new Error("Fichier JSON non trouvé après exécution du script.")
    }

    const content = await readFile(join(outDir, jsonFile), 'utf-8')
    return {
      success: true,
      data: JSON.parse(content)
    }

  } catch (error: any) {
    console.error('ERREUR SCAN:', error)
    return {
      success: false,
      error: error.message || 'Erreur inconnue',
      details: error.toString()
    }
  }
})