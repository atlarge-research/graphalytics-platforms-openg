/*
 * Copyright 2015 Delft University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.tudelft.graphalytics.openg.reporting.logging;

//import org.apache.log4j.FileAppender;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.log4j.PatternLayout;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

/**
 * Created by wlngai on 9-9-15.
 */
public class OpenGLogger extends GraphalyticLogger {

//    protected static Level platformLogLevel = Level.DEBUG;

    private static PrintStream console;


    public static void startPlatformLogging(Path fileName) {
        console = System.out;
        try {
            File file = null;
            file = fileName.toFile();
            file.getParentFile().mkdirs();
            file.createNewFile();
            FileOutputStream fos = new FileOutputStream(file);
            PrintStream ps = new PrintStream(fos);
            System.setOut(ps);
        } catch(Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("cannot redirect to output file");
        }
        System.out.println("StartTime: " + System.currentTimeMillis());


    }

    public static void stopPlatformLogging() {
        System.out.println("EndTime: " + System.currentTimeMillis());
          System.setOut(console);
    }

}
