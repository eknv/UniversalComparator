package com.eknv;

import groovy.lang.Binding;
import groovy.util.GroovyScriptEngine;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.maven.monitor.logging.DefaultLog;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Locale;


@Mojo(name = "compare")
public class MainMojo extends AbstractMojo implements ApplicationContextAware
{

    @Autowired
    ResourceLoader resourceLoader;

    @Autowired
    ApplicationContext applicationContext;

    /**
     * Control configuration
     */
    @Parameter(property = "fail.immediate", defaultValue = "false")
    public static boolean failImmediate;

    @Parameter(property = "consider.column.mismatch", defaultValue = "true")
    public static boolean considerColumnMismatch;

    @Parameter(property = "consider.column.missing", defaultValue = "true")
    public static boolean considerColumnMissing;

    @Parameter(property = "consider.constraints.name.mismatch", defaultValue = "true")
    public static boolean considerConstraintsNameMismatch;

    @Parameter(property = "consider.constraints.missing", defaultValue = "true")
    public static boolean considerConstraintsMissing;

    @Parameter(property = "log.details", defaultValue = "true")
    public static boolean logDetails;

    @Parameter(property = "log.level", defaultValue = "INFO")
    private LogLevel logLevel;

    /**
     * Source configuration
     */
    @Parameter(property = "sources", defaultValue = "ERD, HBM, SQL, DB")
    private String sources;

    /**
     * directory configuration
     */
    private String projectRootFolder;

    @Parameter(readonly = true, defaultValue = "${project}")
    private MavenProject mavenProject;

    @Parameter(property = "erd.re.export.to.xml", defaultValue = "true")
    private boolean erdReExportToXml;

    @Parameter(property = "erd.project.file.path")
    private String erdProjectFilePath;

    @Parameter(property = "erd.visual.paradigm.xml.export.script")
    private String erdVisualParadigmXmlExportScript;

    @Parameter(property = "execution.directory")
    private String executionDirectory;


    /**
     * Table configuration
     */
    @Parameter(property = "system.tables", defaultValue = "SYSCHKCST,SYSCOLUMNS,SYSCST,SYSCSTCOL,SYSCSTDEP,SYSFIELDS,SYSINDEXES,SYSKEYCST,SYSKEYS,SYSPACKAGE,SYSREFCST,SYSREFCST,SYSTABLEDEP,SYSTABLES,SYSTRIGCOL,SYSTRIGDEP,SYSTRIGGERS,SYSTRIGUPD,SYSVIEWDEP,SYSVIEWS")
    private String systemTables;

    @Parameter(property = "tables.to.ignore")
    private String tablesToIgnore;


    /**
     * Database configuration
     */
    @Parameter(property = "db.server.address")
    private String dbServerAddress;

    @Parameter(property = "db.schema.name")
    private String dbSchemaName;

    @Parameter(property = "db.username")
    private String dbUserName;

    @Parameter(property = "db.password")
    private String dbPassword;


    public static void main(String[] args) throws Exception
    {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");

        MainMojo mainMojo = (MainMojo) applicationContext.getBean("mainMojo");

        if ("true".equals(applicationContext.getMessage("fail-immediate", args, Locale.getDefault()).toLowerCase().trim()))
        {
            mainMojo.setFailImmediate(true);
        }
        if ("true".equals(applicationContext.getMessage("consider-column-mismatch", args, Locale.getDefault()).toLowerCase().trim()))
        {
            mainMojo.setConsiderColumnMismatch(true);
        }
        if ("true".equals(applicationContext.getMessage("consider-column-missing", args, Locale.getDefault()).toLowerCase().trim()))
        {
            mainMojo.setConsiderColumnMissing(true);
        }
        if ("true".equals(applicationContext.getMessage("consider-constraints-name-mismatch", args, Locale.getDefault()).toLowerCase().trim()))
        {
            mainMojo.setConsiderConstraintsNameMismatch(true);
        }
        if ("true".equals(applicationContext.getMessage("consider-constraints-missing", args, Locale.getDefault()).toLowerCase().trim()))
        {
            mainMojo.setConsiderConstraintsMissing(true);
        }
        if ("true".equals(applicationContext.getMessage("log-details", args, Locale.getDefault()).toLowerCase().trim()))
        {
            mainMojo.setLogDetails(true);
        }
        mainMojo.setLogLevel(LogLevel.fromString(applicationContext.getMessage("log-level", args, Locale.getDefault())));

        mainMojo.setSources(applicationContext.getMessage("sources", args, Locale.getDefault()));
        mainMojo.setProjectRootFolder(applicationContext.getMessage("project-root-folder", args, Locale.getDefault()));
        if ("true".equals(applicationContext.getMessage("re-export-erd-xml", args, Locale.getDefault()).toLowerCase().trim()))
        {
            mainMojo.setErdReExportToXml(true);
        }
        mainMojo.setErdProjectFilePath(applicationContext.getMessage("erd-project-file-path", args, Locale.getDefault()));
        mainMojo.setErdVisualParadigmXmlExportScript(applicationContext.getMessage("visual-paradigm-xml-export-script", args, Locale.getDefault()));
        mainMojo.setExecutionDirectory(applicationContext.getMessage("user-directory", args, Locale.getDefault()));
        mainMojo.setSystemTables(applicationContext.getMessage("sys-tables", args, Locale.getDefault()));
        mainMojo.setTablesToIgnore(applicationContext.getMessage("tables-to-ignore", args, Locale.getDefault()));
        mainMojo.setDbServerAddress(applicationContext.getMessage("server-address", args, Locale.getDefault()));
        mainMojo.setDbSchemaName(applicationContext.getMessage("schema-name", args, Locale.getDefault()));
        mainMojo.setDbUserName(applicationContext.getMessage("username", args, Locale.getDefault()));
        mainMojo.setDbPassword(applicationContext.getMessage("password", args, Locale.getDefault()));

        mainMojo.execute();
    }


    public void execute() throws MojoFailureException
    {
        Log logger = new DefaultLog(new ConsoleLogger(getLogLevel().getThreshold(), "MainMojo"));

        try
        {
            Binding binding = new Binding();
            binding.setVariable("logger", logger);
            binding.setVariable("failImmediate", failImmediate);
            binding.setVariable("considerColumnMismatch", considerColumnMismatch);
            binding.setVariable("considerColumnMissing", considerColumnMissing);
            binding.setVariable("considerConstraintsNameMismatch", considerConstraintsNameMismatch);
            binding.setVariable("considerConstraintsMissing", considerConstraintsMissing);
            binding.setVariable("logDetails", logDetails);
            binding.setVariable("logLevel", getLogLevel().toString());
            binding.setVariable("sources", sources);
            binding.setVariable("projectRootFolder", projectRootFolder);
            binding.setVariable("erdReExportToXml", erdReExportToXml);
            binding.setVariable("erdProjectFilePath", erdProjectFilePath);
            binding.setVariable("erdVisualParadigmXmlExportScript", erdVisualParadigmXmlExportScript);
            binding.setVariable("executionDirectory", executionDirectory);
            binding.setVariable("systemTables", systemTables);
            binding.setVariable("tablesToIgnore", tablesToIgnore);
            binding.setVariable("dbServerAddress", dbServerAddress);
            binding.setVariable("dbSchemaName", dbSchemaName);
            binding.setVariable("dbUserName", dbUserName);
            binding.setVariable("dbPassword", dbPassword);

            /**
             * resourceLoader is not set if the Mojo is being run as a maven plugin
             */
            if (resourceLoader == null)
            {
                ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");
                MainMojo mainMojo = (MainMojo) applicationContext.getBean("mainMojo");
                new NullAwareBeanUtilsBean().copyProperties(mainMojo, this);
                mainMojo.setProjectRootFolder(mavenProject.getBasedir().getAbsolutePath());
                mainMojo.execute();
            }
            else
            {
                Resource resource = resourceLoader.getResource("classpath:groovy/");
                GroovyScriptEngine groovyScriptEngine = new GroovyScriptEngine(new URL[]{resource.getURL()});
                groovyScriptEngine.run("Comparison.groovy", binding);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public String getSources()
    {
        return sources;
    }

    public void setSources(String sources)
    {
        this.sources = sources;
    }

    public String getProjectRootFolder()
    {
        return projectRootFolder;
    }

    public void setProjectRootFolder(String projectRootFolder)
    {
        this.projectRootFolder = projectRootFolder;
    }

    public MavenProject getMavenProject()
    {
        return mavenProject;
    }

    public void setMavenProject(MavenProject mavenProject)
    {
        this.mavenProject = mavenProject;
    }

    public boolean getErdReExportToXml()
    {
        return erdReExportToXml;
    }

    public void setErdReExportToXml(boolean erdReExportToXml)
    {
        this.erdReExportToXml = erdReExportToXml;
    }

    public String getErdProjectFilePath()
    {
        return erdProjectFilePath;
    }

    public void setErdProjectFilePath(String erdProjectFilePath)
    {
        this.erdProjectFilePath = erdProjectFilePath;
    }

    public String getErdVisualParadigmXmlExportScript()
    {
        return erdVisualParadigmXmlExportScript;
    }

    public void setErdVisualParadigmXmlExportScript(String erdVisualParadigmXmlExportScript)
    {
        this.erdVisualParadigmXmlExportScript = erdVisualParadigmXmlExportScript;
    }

    public String getExecutionDirectory()
    {
        return executionDirectory;
    }

    public void setExecutionDirectory(String executionDirectory)
    {
        this.executionDirectory = executionDirectory;
    }

    public String getSystemTables()
    {
        return systemTables;
    }

    public void setSystemTables(String systemTables)
    {
        this.systemTables = systemTables;
    }

    public String getTablesToIgnore()
    {
        return tablesToIgnore;
    }

    public void setTablesToIgnore(String tablesToIgnore)
    {
        this.tablesToIgnore = tablesToIgnore;
    }

    public String getDbServerAddress()
    {
        return dbServerAddress;
    }

    public void setDbServerAddress(String dbServerAddress)
    {
        this.dbServerAddress = dbServerAddress;
    }

    public String getDbSchemaName()
    {
        return dbSchemaName;
    }

    public void setDbSchemaName(String dbSchemaName)
    {
        this.dbSchemaName = dbSchemaName;
    }

    public String getDbUserName()
    {
        return dbUserName;
    }

    public void setDbUserName(String dbUserName)
    {
        this.dbUserName = dbUserName;
    }

    public String getDbPassword()
    {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword)
    {
        this.dbPassword = dbPassword;
    }

    public static boolean getFailImmediate()
    {
        return failImmediate;
    }

    public static void setFailImmediate(boolean failImmediate)
    {
        MainMojo.failImmediate = failImmediate;
    }

    public static boolean isConsiderColumnMismatch()
    {
        return considerColumnMismatch;
    }

    public static void setConsiderColumnMismatch(boolean considerColumnMismatch)
    {
        MainMojo.considerColumnMismatch = considerColumnMismatch;
    }

    public static boolean isConsiderColumnMissing()
    {
        return considerColumnMissing;
    }

    public static void setConsiderColumnMissing(boolean considerColumnMissing)
    {
        MainMojo.considerColumnMissing = considerColumnMissing;
    }

    public static boolean isConsiderConstraintsNameMismatch()
    {
        return considerConstraintsNameMismatch;
    }

    public static void setConsiderConstraintsNameMismatch(boolean considerConstraintsNameMismatch)
    {
        MainMojo.considerConstraintsNameMismatch = considerConstraintsNameMismatch;
    }

    public static boolean getConsiderConstraintsMissing()
    {
        return considerConstraintsMissing;
    }

    public static void setConsiderConstraintsMissing(boolean considerConstraintsMissing)
    {
        MainMojo.considerConstraintsMissing = considerConstraintsMissing;
    }

    public static boolean isLogDetails()
    {
        return logDetails;
    }

    public static void setLogDetails(boolean logDetails)
    {
        MainMojo.logDetails = logDetails;
    }

    public LogLevel getLogLevel()
    {
        return logLevel;
    }

    public void setLogLevel(LogLevel logLevel)
    {
        this.logLevel = logLevel;
    }

    public void setResourceLoader(ResourceLoader resourceLoader)
    {
        this.resourceLoader = resourceLoader;
    }

    public void setApplicationContext(ApplicationContext applicationContext)
    {
        this.applicationContext = applicationContext;
    }

    public class NullAwareBeanUtilsBean extends BeanUtilsBean
    {

        @Override
        public void copyProperty(Object dest, String name, Object value)
                throws IllegalAccessException, InvocationTargetException
        {
            if (value == null) return;
            super.copyProperty(dest, name, value);
        }
    }


    public enum LogLevel
    {
        FATAL(4),
        ERROR(3),
        WARN(2),
        INFO(1),
        DEBUG(0);

        int threshold;

        LogLevel(int threshold)
        {
            this.threshold = threshold;
        }

        public static LogLevel fromString(String string)
        {
            if (string == null)
            {
                return null;
            }
            for (LogLevel logLevel : values())
            {
                if (logLevel.toString().equalsIgnoreCase(string))
                {
                    return logLevel;
                }
            }
            return null;
        }

        public int getThreshold()
        {
            return threshold;
        }

        public void setThreshold(int threshold)
        {
            this.threshold = threshold;
        }
    }

}


