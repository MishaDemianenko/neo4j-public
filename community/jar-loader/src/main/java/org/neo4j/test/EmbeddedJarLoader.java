package org.neo4j.test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class EmbeddedJarLoader
{

    private String embeddedJarLocation;
    private File extractedFile;

    private URLClassLoader urlClassLoader;

    public EmbeddedJarLoader( String embeddedJarLocation )
    {
        this.embeddedJarLocation = embeddedJarLocation;
    }

    public Class<?> loadClass( String className ) throws ClassNotFoundException, IOException
    {
        if ( urlClassLoader == null )
        {
            urlClassLoader = buildJarClassLoader();
        }
        return urlClassLoader.loadClass( className );
    }

    private URLClassLoader buildJarClassLoader() throws IOException
    {
        File jarFile = extractJarFile();
        return new URLClassLoader( new URL[]{jarFile.toURI().toURL()}, this.getClass().getClassLoader() );
    }

    private File extractJarFile() throws IOException
    {
        URL url = getClass().getClassLoader().getResource( embeddedJarLocation );
        JarURLConnection urlConnection = (JarURLConnection) url.openConnection();
        JarFile jarFile = urlConnection.getJarFile();
        JarEntry jarEntry = urlConnection.getJarEntry();

        extractedFile = createTempFile( jarEntry );

        try ( InputStream jarInputStream = jarFile.getInputStream( jarEntry ) )
        {
            Files.copy( jarInputStream, extractedFile.toPath(), StandardCopyOption.REPLACE_EXISTING );
        }
        return this.extractedFile;
    }

    private File createTempFile( JarEntry jarEntry ) throws IOException
    {
        File tempFile = File.createTempFile( jarEntry.getName(), "jar" );
        tempFile.deleteOnExit();
        return tempFile;
    }

    public void close() throws IOException
    {
        if ( urlClassLoader != null )
        {
            urlClassLoader.close();
        }
        if ( extractedFile != null )
        {
            if ( !extractedFile.delete() )
            {
                // we can't delete file, it will be deleted on exit then
            }
        }
    }
}
