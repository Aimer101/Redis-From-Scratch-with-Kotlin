import java.io.IOException
import java.nio.file.Paths
import java.nio.file.Files
import java.io.FileInputStream

class RDB {

    companion object {
        val HEADER_SECTION = "REDIS0011"
        val METADATA_SECTION = 0xFA.toByte()
        val DATABASE_SECTION = 0xFE.toByte()
        val HAST_TABLE_INFO = 0xFB.toByte()
        val STRING_ENCODING = 0x00.toByte()
        val EXPIRY_IN_MS = 0xFC.toByte() // 8 bytes
        val EXPIRY_IN_S = 0xFD.toByte() // 4 bytes
    }

    private fun handleMatchingKeyPattern(pattern: String, fis: FileInputStream, matchingKeys: MutableList<String>) : Boolean {
        val keyLength = fis.read()
        if (keyLength == -1) return false

        val keyBytes = ByteArray(keyLength)
        if (fis.read(keyBytes) != keyLength) return false

        val key = String(keyBytes)

        if (key.matches(pattern)) {
            matchingKeys.add(key)
        }

        val valueLength = fis.read()
        if (valueLength == -1) return false

        fis.skip(valueLength.toLong())
        return true
    }

    fun getAllKeysMatchingPattern(pattern: String): List<String> {
        val dbPath = ServerConfig.getDbFilePath()
        val matchingKeys= mutableListOf<String>()
        val regexPattern = pattern.replace("*", ".*").toRegex()
        var isDatabaseSection = false
        var byteRead: Int

        try {
            FileInputStream(dbPath).use { fis ->
                while(fis.read().also { byteRead = it } != -1) {
                    if (byteRead.toByte() == DATABASE_SECTION) {
                        isDatabaseSection = true
                    }

                    if (isDatabaseSection) {
                        // for now ignore metadata section which is 4 bytes according to the challenge description
                        fis.skip(4)

                        while (fis.read().also { byteRead = it } != -1) {
                            when(byteRead.toByte()) {
                                STRING_ENCODING -> {
                                    val handleMatching = handleMatchingKeyPattern(regexPattern, fis, matchingKeys)

                                    if (!handleMatching) {
                                        break
                                    }
                                }
                                EXPIRY_IN_MS -> {
                                    fis.skip(8) // Skip the 8-byte expiry timestamp
                                }
                                EXPIRY_IN_S -> {
                                    fis.skip(4) // Skip the 4-byte expiry timestamp
                                }
                            }

                        }
                    }
                }
            }
        } catch (e: IOException) {
            println("Error while reading RDB file: $e")
        }

        return matchingKeys
    }

    fun createPersistence(args : Array<String>) {
        var dir: String
        var dbfilename: String

        for(i in args.indices){
            when(args[i]){
                "--dir" -> dir = args[i + 1]
                "--dbfilename" -> dbfilename = args[i + 1]
            }
        }

        ServerConfig.set("DIR", dir)
        ServerConfig.set("DBFILENAME", dbfilename)

        // Create directory if it doesn't exist
        val dirPath = Paths.get(dir)
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath)
            println("Directory created: $dir")
        } else {
            println("Directory already exists: $dir")
        }

        // Create database file
        val dbFilePath = Paths.get(dir, dbfilename)
        if (!Files.exists(dbFilePath)) {
            Files.createFile(dbFilePath)
            println("Database file created: $dbfilename")
        } else {
            println("Database file already exists: $dbfilename")
        }
    }
}