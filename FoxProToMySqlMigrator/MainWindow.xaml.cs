using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Windows;
using System.ComponentModel;
using System.Windows.Media;
using System.Windows.Media.Animation;
using Microsoft.Win32;
using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private readonly FoxProMigrationService _migrationService;
        private bool _isMigrating;
        private CancellationTokenSource? _cancellationTokenSource;
        private MigrationCheckpoint? _currentCheckpoint;
        private System.Timers.Timer? _watchdogTimer;
        private DateTime _lastLogUpdate;
        private bool _frozenAlertShown;
        private int _retryCount = 0;
        private const int MaxRetries = 3;
        private bool _shouldRetryRequested = false;
        private System.Windows.Controls.ListBoxItem? _currentBatchLogItem;
        private bool _settingsLoaded;
        private List<string> _neededTables = new();
        private bool _neededTablesFileLoaded;

        private sealed class NeededTablesFile
        {
            public List<string> Tables { get; set; } = new();
        }

        public MainWindow()
        {
            InitializeComponent();
            _migrationService = new FoxProMigrationService();
            _migrationService.LogMessage += OnLogMessage;
            _migrationService.TableCompleted += OnTableCompleted;
            
            SetApplicationVersion();
            LoadDefaultSettings();
            LoadNeededTablesFromJson(showSuccessMessage: false);
            SetupWatchdog();
        }

        private void SetApplicationVersion()
        {
            var assembly = Assembly.GetExecutingAssembly();
            var version = assembly
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
                .InformationalVersion
                ?? assembly.GetName().Version?.ToString()
                ?? "0.0.0.0";
            var metadataIndex = version.IndexOf('+');
            if (metadataIndex >= 0)
            {
                version = version[..metadataIndex];
            }

            TxtAppVersion.Text = $"v{version}";
            Title = $"FoxPro to MySQL Migrator v{version}";
        }

        private void SetupWatchdog()
        {
            _watchdogTimer = new System.Timers.Timer(30000); // Check every 30 seconds
            _watchdogTimer.Elapsed += (s, e) =>
            {
                if (_isMigrating && !_frozenAlertShown && (DateTime.Now - _lastLogUpdate).TotalMinutes > 5)
                {
                    _frozenAlertShown = true;
                    Dispatcher.Invoke(() =>
                    {
                        var result = MessageBox.Show(
                            "⚠️ MIGRATION APPEARS FROZEN\n\n" +
                            "No activity detected for 5 minutes.\n" +
                            "This may indicate:\n" +
                            "• Database connection timeout\n" +
                            "• Network issues\n" +
                            "• Very large batch processing\n\n" +
                            "The app can attempt to retry the migration.\n\n" +
                            "Click Yes to cancel and retry automatically.\n" +
                            "Click No to keep waiting.",
                            "Migration May Be Frozen",
                            MessageBoxButton.YesNo,
                            MessageBoxImage.Warning);

                        if (result == MessageBoxResult.Yes)
                        {
                            // Request a retry; the cancellation handler will perform the restart after cleanup
                            _shouldRetryRequested = true;
                            _cancellationTokenSource?.Cancel();
                        }
                    });
                }
            };
        }

        private void LoadDefaultSettings()
        {
            var config = UserAppConfigStore.Load();

            TxtMySqlServer.Text = config.MySqlServer;
            TxtDatabaseName.Text = config.TargetDatabase;
            TxtFoxProFolder.Text = config.FoxProFolder;
            TxtBatchSize.Text = config.BatchSize.ToString();
            ChkOnlySelectedTables.IsChecked = config.TableFilterEnabled;
            _settingsLoaded = true;
        }

        private void SaveCurrentSettings()
        {
            if (!_settingsLoaded)
            {
                return;
            }

            var config = new UserAppConfig
            {
                MySqlServer = TxtMySqlServer.Text,
                TargetDatabase = TxtDatabaseName.Text,
                FoxProFolder = TxtFoxProFolder.Text,
                BatchSize = int.TryParse(TxtBatchSize.Text, out var batchSize) ? batchSize : 1000,
                TableFilterEnabled = ChkOnlySelectedTables.IsChecked == true
            };

            UserAppConfigStore.Save(config);
        }

        private IReadOnlyCollection<string>? GetActiveTableFilter()
        {
            if (ChkOnlySelectedTables.IsChecked != true)
            {
                return null;
            }

            // When filtering is enabled, a missing file is an empty allowlist.
            // Validation stops the operation instead of migrating every DBF.
            return _neededTablesFileLoaded ? _neededTables : Array.Empty<string>();
        }

        private void UpdateSelectedTableCount()
        {
            if (TxtSelectedTableCount == null)
            {
                return;
            }

            if (ChkOnlySelectedTables.IsChecked != true)
            {
                TxtSelectedTableCount.Text = "Allowlist OFF - all DBF tables will be migrated";
            }
            else if (!_neededTablesFileLoaded)
            {
                TxtSelectedTableCount.Text = "Allowlist file missing; migration is disabled";
            }
            else
            {
                var count = _neededTables.Count;
                TxtSelectedTableCount.Text = count == 1
                    ? "1 table selected"
                    : $"{count:N0} tables selected";
            }
        }

        private void LoadNeededTablesFromJson(bool showSuccessMessage)
        {
            try
            {
                var path = GetNeededTablesJsonPath();
                if (!File.Exists(path))
                {
                    _neededTablesFileLoaded = false;
                    _neededTables = new List<string>();
                    LstNeededTables.ItemsSource = _neededTables;
                    UpdateSelectedTableCount();

                    if (showSuccessMessage)
                    {
                        MessageBox.Show("needed-tables.json was not found. If the allowlist is enabled, migration will be blocked until the file is available.", "Reload JSON", MessageBoxButton.OK, MessageBoxImage.Warning);
                    }

                    return;
                }

                var json = ReadNeededTablesJson(path);
                var tableFile = JsonSerializer.Deserialize<NeededTablesFile>(json, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });

                _neededTables = (tableFile?.Tables ?? new List<string>())
                    .Select(table => table.Trim().Trim('`', '"', '\'').ToLowerInvariant())
                    .Where(table => !string.IsNullOrWhiteSpace(table))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(table => table)
                    .ToList();

                _neededTablesFileLoaded = true;
                LstNeededTables.ItemsSource = _neededTables;
                UpdateSelectedTableCount();

                if (showSuccessMessage)
                {
                    MessageBox.Show($"Loaded {_neededTables.Count:N0} table(s) from needed-tables.json.", "Reload JSON", MessageBoxButton.OK, MessageBoxImage.Information);
                }
            }
            catch (Exception ex)
            {
                _neededTablesFileLoaded = false;
                _neededTables = new List<string>();
                LstNeededTables.ItemsSource = _neededTables;
                UpdateSelectedTableCount();
                MessageBox.Show($"Could not load needed-tables.json.\n\n{ex.Message}", "Table Inventory", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private static string GetNeededTablesJsonPath()
        {
            return Path.Combine(AppSettings.AppDataFolder, "needed-tables.json");
        }

        private static string ReadNeededTablesJson(string path)
        {
            if (!File.Exists(path))
            {
                throw new FileNotFoundException("needed-tables.json was not found.", path);
            }

            return File.ReadAllText(path);
        }

        private void BtnReloadNeededTables_Click(object sender, RoutedEventArgs e)
        {
            LoadNeededTablesFromJson(showSuccessMessage: true);
        }

        private void BtnOpenNeededTablesJson_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                Directory.CreateDirectory(AppSettings.AppDataFolder);

                Process.Start(new ProcessStartInfo
                {
                    FileName = File.Exists(GetNeededTablesJsonPath())
                        ? GetNeededTablesJsonPath()
                        : AppSettings.AppDataFolder,
                    UseShellExecute = true
                });
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Could not open needed-tables.json.\n\n{ex.Message}", "Open JSON", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void TableFilterSettings_Changed(object sender, RoutedEventArgs e)
        {
            if (!_settingsLoaded)
            {
                return;
            }

            UpdateSelectedTableCount();
            SaveCurrentSettings();
        }

        private async void CheckForExistingCheckpoint()
        {
            if (string.IsNullOrWhiteSpace(TxtFoxProFolder.Text) || 
                string.IsNullOrWhiteSpace(TxtDatabaseName.Text))
            {
                CheckpointNotification.Visibility = Visibility.Collapsed;
                return;
            }

            var checkpoint = await _migrationService.LoadCheckpointAsync(
                TxtFoxProFolder.Text, 
                TxtDatabaseName.Text);

            if (checkpoint != null)
            {
                _currentCheckpoint = checkpoint;
                var failedCount = checkpoint.FailedTables?.Count ?? 0;
                TxtCheckpointMessage.Text = $"Found incomplete migration: {checkpoint.CompletedTables.Count} of {checkpoint.TotalTables} tables completed" +
                                           (failedCount > 0 ? $", {failedCount} failed/skipped" : "") +
                                           $". Started: {checkpoint.StartTime:yyyy-MM-dd HH:mm:ss}";
                CheckpointNotification.Visibility = Visibility.Visible;
            }
            else
            {
                _currentCheckpoint = null;
                CheckpointNotification.Visibility = Visibility.Collapsed;
            }
        }

        private void TxtFoxProFolder_TextChanged(object sender, System.Windows.Controls.TextChangedEventArgs e)
        {
            SaveCurrentSettings();
            CheckForExistingCheckpoint();
        }

        private void TxtDatabaseName_TextChanged(object sender, System.Windows.Controls.TextChangedEventArgs e)
        {
            SaveCurrentSettings();
            CheckForExistingCheckpoint();
        }

        private void TxtSettings_TextChanged(object sender, System.Windows.Controls.TextChangedEventArgs e)
        {
            SaveCurrentSettings();
        }

        private void StartSpinner()
        {
            LoadingSpinner.Visibility = Visibility.Visible;
            var spinnerStoryboard = (Storyboard)FindResource("SpinnerAnimation");
            spinnerStoryboard.Begin(this, true);
        }

        private void StopSpinner()
        {
            var spinnerStoryboard = (Storyboard)FindResource("SpinnerAnimation");
            spinnerStoryboard.Stop(this);
            LoadingSpinner.Visibility = Visibility.Collapsed;
            LoadingText.Text = "Migrating...";
        }

        private void BrowseFolder_Click(object sender, RoutedEventArgs e)
        {
            var dialog = new OpenFolderDialog
            {
                Title = "Select FoxPro Database Folder",
                Multiselect = false
            };

            if (dialog.ShowDialog() == true)
            {
                TxtFoxProFolder.Text = dialog.FolderName;
                SaveCurrentSettings();
            }
        }

        private async void BtnResumeCheckpoint_Click(object sender, RoutedEventArgs e)
        {
            if (_currentCheckpoint == null)
                return;

            var result = MessageBox.Show(
                $"Resume migration from checkpoint?\n\n" +
                $"Already completed: {_currentCheckpoint.CompletedTables.Count}/{_currentCheckpoint.TotalTables} tables\n" +
                $"Failed/skipped: {_currentCheckpoint.FailedTables?.Count ?? 0}\n" +
                $"Started: {_currentCheckpoint.StartTime:yyyy-MM-dd HH:mm:ss}\n\n" +
                $"This will continue from where the migration was stopped.",
                "Resume Migration",
                MessageBoxButton.YesNo,
                MessageBoxImage.Question);

            if (result == MessageBoxResult.Yes)
            {
                await StartMigrationAsync(_currentCheckpoint);
            }
        }

        private async void BtnMigrate_Click(object sender, RoutedEventArgs e)
        {
            if (_isMigrating)
            {
                MessageBox.Show("Migration is already in progress.", "Please Wait", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            // Reset retry counter for a fresh user-initiated migration
            _retryCount = 0;

            if (string.IsNullOrWhiteSpace(TxtFoxProFolder.Text))
            {
                MessageBox.Show("Please select a FoxPro folder.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            if (string.IsNullOrWhiteSpace(TxtMySqlServer.Text))
            {
                MessageBox.Show("Please enter MySQL server connection details.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            if (string.IsNullOrWhiteSpace(TxtDatabaseName.Text))
            {
                MessageBox.Show("Please enter a target database name.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            // Validate and parse batch size
            if (!int.TryParse(TxtBatchSize.Text, out int batchSize) || batchSize < 1 || batchSize > 10000)
            {
                MessageBox.Show("Batch size must be a number between 1 and 10,000.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            var selectedTables = GetActiveTableFilter();
            if (selectedTables != null && selectedTables.Count == 0)
            {
                MessageBox.Show("The table filter is enabled, but no tables are listed. Add table names or turn off the filter.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            // Check if there's an existing checkpoint and ask user
            if (_currentCheckpoint != null)
            {
                var result = MessageBox.Show(
                    $"Found an incomplete migration:\n\n" +
                    $"Already completed: {_currentCheckpoint.CompletedTables.Count}/{_currentCheckpoint.TotalTables} tables\n" +
                    $"Failed/skipped: {_currentCheckpoint.FailedTables?.Count ?? 0}\n" +
                    $"Started: {_currentCheckpoint.StartTime:yyyy-MM-dd HH:mm:ss}\n\n" +
                    $"Do you want to:\n" +
                    $"• YES - Resume from checkpoint\n" +
                    $"• NO - Start fresh (checkpoint will be overwritten)",
                    "Resume or Start Fresh?",
                    MessageBoxButton.YesNoCancel,
                    MessageBoxImage.Question);

                if (result == MessageBoxResult.Cancel)
                    return;

                if (result == MessageBoxResult.Yes)
                {
                    await StartMigrationAsync(_currentCheckpoint);
                    return;
                }
                else
                {
                    // User chose to start fresh
                    _currentCheckpoint = null;
                }
            }

            SaveCurrentSettings();
            await StartMigrationAsync(null);
        }

        private async Task StartMigrationAsync(MigrationCheckpoint? resumeFromCheckpoint)
        {
            SaveCurrentSettings();

            bool requestRetryAfterCleanup = false;
            MigrationCheckpoint? checkpointToRetry = null;
            try
            {
                _isMigrating = true;
                _lastLogUpdate = DateTime.Now;
                _frozenAlertShown = false;
                _cancellationTokenSource = new CancellationTokenSource();
                
                // Start watchdog
                _watchdogTimer?.Start();
                
                // Update UI
                BtnMigrate.IsEnabled = false;
                BtnUpdateTypes.IsEnabled = false;
                BtnStop.Visibility = Visibility.Visible;
                CheckpointNotification.Visibility = Visibility.Collapsed;
                StartSpinner();

                var migrationMode = MigrationMode.FullReload;

                // Build connection string with database
                var connectionString = TxtMySqlServer.Text.TrimEnd(';') + $";Database={TxtDatabaseName.Text};";

                // Parse batch size
                int.TryParse(TxtBatchSize.Text, out int batchSize);

                var foxProFolder = TxtFoxProFolder.Text;
                var databaseName = TxtDatabaseName.Text;
                var tableFilter = GetActiveTableFilter();
                var cancellationToken = _cancellationTokenSource.Token;

                // DBF scanning is CPU-bound and can run for a long time on large
                // files. Keep it off the WPF dispatcher so the window and Stop
                // button remain responsive throughout the scan.
                await Task.Run(() => _migrationService.MigrateAsync(
                    foxProFolder,
                    connectionString,
                    databaseName,
                    true,
                    migrationMode,
                    batchSize,
                    tableFilter,
                    resumeFromCheckpoint,
                    cancellationToken
                ), cancellationToken);

                if (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    MessageBox.Show("Migration completed successfully!", "Success", MessageBoxButton.OK, MessageBoxImage.Information);
                    _currentCheckpoint = null;
                }
            }
            catch (OperationCanceledException)
            {
                // If a watchdog requested a retry, we will attempt to restart after cleanup
                if (_shouldRetryRequested)
                {
                    _shouldRetryRequested = false;
                    _retryCount++;
                    if (_retryCount <= MaxRetries)
                    {
                        requestRetryAfterCleanup = true;
                        // Load the checkpoint to resume from where possible
                        checkpointToRetry = await _migrationService.LoadCheckpointAsync(TxtFoxProFolder.Text, TxtDatabaseName.Text);
                        OnLogMessage($"Watchdog requested retry #{_retryCount} of {MaxRetries}...");
                    }
                    else
                    {
                        MessageBox.Show($"Migration cancelled and maximum retries ({MaxRetries}) reached.", "Cancelled", MessageBoxButton.OK, MessageBoxImage.Warning);
                    }
                }
                else
                {
                    MessageBox.Show(
                        "Migration was cancelled. Progress has been saved and you can resume later.", 
                        "Cancelled", 
                        MessageBoxButton.OK, 
                        MessageBoxImage.Warning);
                    // Reload checkpoint after cancellation
                    CheckForExistingCheckpoint();
                }
            }
            catch (TimeoutException tex)
            {
                var errorMessage = $"⏱️ TIMEOUT ERROR\n\n" +
                                  $"{tex.Message}\n\n" +
                                  $"✅ Your progress has been saved!\n" +
                                  $"✅ You can safely close and restart the application\n" +
                                  $"✅ Resume migration later from where it stopped\n\n" +
                                  $"💡 Try:\n" +
                                  $"• Reducing batch size (currently {TxtBatchSize.Text})\n" +
                                  $"• Checking database server performance\n" +
                                  $"• Checking network connection\n\n" +
                                  $"📁 Check the log files for more details:\n" +
                                  $"   {AppSettings.LogsFolder}";
                
                MessageBox.Show(errorMessage, "Migration Timeout - Safe to Restart", MessageBoxButton.OK, MessageBoxImage.Error);
                CheckForExistingCheckpoint();
            }
            catch (Exception ex)
            {
                var errorMessage = $"⚠️ MIGRATION ERROR\n\n" +
                                  $"Error: {ex.Message}\n\n";
                
                if (ex.InnerException != null)
                {
                    errorMessage += $"Details: {ex.InnerException.Message}\n\n";
                }

                errorMessage += $"✅ Your progress has been saved!\n" +
                               $"✅ You can safely close and restart the application\n" +
                               $"✅ Resume migration later from where it stopped\n\n" +
                               $"📁 Check the log files for more details:\n" +
                               $"   {AppSettings.LogsFolder}\n\n" +
                               $"Would you like to retry the migration?";

                var retryResult = MessageBox.Show(errorMessage, "Migration Error - Retry?", MessageBoxButton.YesNo, MessageBoxImage.Error);

                if (retryResult == MessageBoxResult.Yes && _retryCount < MaxRetries)
                {
                    _retryCount++;
                    requestRetryAfterCleanup = true;
                    checkpointToRetry = await _migrationService.LoadCheckpointAsync(TxtFoxProFolder.Text, TxtDatabaseName.Text);
                }
                else
                {
                    // Reload checkpoint after error
                    CheckForExistingCheckpoint();
                }
            }
            finally
            {
                _isMigrating = false;
                _watchdogTimer?.Stop();
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
                
                // Restore UI
                BtnMigrate.IsEnabled = true;
                BtnUpdateTypes.IsEnabled = true;
                BtnStop.Visibility = Visibility.Collapsed;
                BtnStop.IsEnabled = true;
                StopSpinner();
            }

            // If a retry was requested (either by watchdog or user on error), start again after cleanup
            if (requestRetryAfterCleanup && checkpointToRetry != null)
            {
                // Small delay to ensure UI has settled
                await Task.Delay(1500);
                await StartMigrationAsync(checkpointToRetry);
            }
        }

        private async void BtnUpdateTypes_Click(object sender, RoutedEventArgs e)
        {
            if (_isMigrating)
            {
                MessageBox.Show("A migration or update is already in progress.", "Please Wait", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            if (!ValidateConnectionInputs())
            {
                return;
            }

            var selectedTables = GetActiveTableFilter();
            if (selectedTables != null && selectedTables.Count == 0)
            {
                MessageBox.Show("The table filter is enabled, but needed-tables.json did not load any tables.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            var result = MessageBox.Show(
                "Update existing MySQL text column types from the DBF schema?\n\n" +
                "This will only change existing tables. It will shrink LONGTEXT/TEXT to VARCHAR only when current MySQL values fit the DBF field length. Unsafe columns are skipped and logged.",
                "Update Data Types",
                MessageBoxButton.YesNo,
                MessageBoxImage.Question);

            if (result != MessageBoxResult.Yes)
            {
                return;
            }

            SaveCurrentSettings();

            try
            {
                _isMigrating = true;
                _lastLogUpdate = DateTime.Now;
                _frozenAlertShown = false;
                _cancellationTokenSource = new CancellationTokenSource();

                BtnMigrate.IsEnabled = false;
                BtnUpdateTypes.IsEnabled = false;
                BtnStop.Visibility = Visibility.Visible;
                LoadingText.Text = "Updating data types...";
                StartSpinner();

                var connectionString = TxtMySqlServer.Text.TrimEnd(';') + $";Database={TxtDatabaseName.Text};";
                await _migrationService.UpdateExistingTableDataTypesAsync(
                    TxtFoxProFolder.Text,
                    connectionString,
                    TxtDatabaseName.Text,
                    true,
                    GetActiveTableFilter(),
                    _cancellationTokenSource.Token);

                MessageBox.Show("Data type update completed. Check the log for updated and skipped columns.", "Update Data Types", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            catch (OperationCanceledException)
            {
                MessageBox.Show("Data type update was cancelled.", "Update Data Types", MessageBoxButton.OK, MessageBoxImage.Warning);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Could not update data types.\n\n{ex.Message}", "Update Data Types", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                _isMigrating = false;
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;

                BtnMigrate.IsEnabled = true;
                BtnUpdateTypes.IsEnabled = true;
                BtnStop.Visibility = Visibility.Collapsed;
                BtnStop.IsEnabled = true;
                StopSpinner();
            }
        }

        private bool ValidateConnectionInputs()
        {
            if (string.IsNullOrWhiteSpace(TxtFoxProFolder.Text))
            {
                MessageBox.Show("Please select a FoxPro folder.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                return false;
            }

            if (string.IsNullOrWhiteSpace(TxtMySqlServer.Text))
            {
                MessageBox.Show("Please enter MySQL server connection details.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                return false;
            }

            if (string.IsNullOrWhiteSpace(TxtDatabaseName.Text))
            {
                MessageBox.Show("Please enter a target database name.", "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                return false;
            }

            return true;
        }

        private void BtnStop_Click(object sender, RoutedEventArgs e)
        {
            if (_cancellationTokenSource != null && !_cancellationTokenSource.IsCancellationRequested)
            {
                var result = MessageBox.Show(
                    "Are you sure you want to stop the migration?\n\n" +
                    "Your progress will be saved and you can resume later.",
                    "Confirm Stop",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Warning);

                if (result == MessageBoxResult.Yes)
                {
                    _cancellationTokenSource.Cancel();
                    LoadingText.Text = "Stopping...";
                    BtnStop.IsEnabled = false;
                }
            }
        }

        private void BtnClearLog_Click(object sender, RoutedEventArgs e)
        {
            LstLog.Items.Clear();
            LstTableSummary.Items.Clear();
            _currentBatchLogItem = null;
        }

        private void BtnOpenLogsFolder_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var dataFolder = GetDataFolder();
                Directory.CreateDirectory(dataFolder);
                Directory.CreateDirectory(AppSettings.LogsFolder);

                Process.Start(new ProcessStartInfo
                {
                    FileName = dataFolder,
                    UseShellExecute = true
                });
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Could not open the data folder.\n\n{ex.Message}",
                    "Open Data Folder",
                    MessageBoxButton.OK,
                MessageBoxImage.Error);
            }
        }

        private void BtnDeleteHistory_Click(object sender, RoutedEventArgs e)
        {
            if (_isMigrating)
            {
                MessageBox.Show(
                    "Stop the migration before deleting logs and migration history.",
                    "Delete Logs & History",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            var result = MessageBox.Show(
                "Delete all logs and saved migration history?\n\n" +
                "This removes checkpoints, so the next migration will start clean instead of resuming.",
                "Delete Logs & History",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning);

            if (result != MessageBoxResult.Yes)
            {
                return;
            }

            try
            {
                DeleteLogsAndMigrationHistory();
                _currentCheckpoint = null;
                CheckpointNotification.Visibility = Visibility.Collapsed;
                LstLog.Items.Clear();
                LstTableSummary.Items.Clear();
                _currentBatchLogItem = null;

                MessageBox.Show(
                    "Logs and migration history were deleted. The next migration will start clean.",
                    "Delete Logs & History",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Could not delete logs and migration history.\n\n{ex.Message}",
                    "Delete Logs & History",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error);
            }
        }

        private static void DeleteLogsAndMigrationHistory()
        {
            var logsFolder = Path.GetFullPath(AppSettings.LogsFolder);
            var appDataFolder = Path.GetFullPath(AppSettings.AppDataFolder);

            if (!logsFolder.StartsWith(appDataFolder, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("Refusing to delete a folder outside the app data directory.");
            }

            if (Directory.Exists(logsFolder))
            {
                Directory.Delete(logsFolder, recursive: true);
            }

            Directory.CreateDirectory(logsFolder);
        }

        private static string GetDataFolder()
        {
            return AppSettings.AppDataFolder;
        }

        private void OnLogMessage(string message)
        {
            Dispatcher.Invoke(() =>
            {
                var logItem = new System.Windows.Controls.ListBoxItem
                {
                    Content = message,
                    Foreground = GetLogMessageBrush(message)
                };

                if (ShouldUpdateCurrentBatchLog(message))
                {
                    if (_currentBatchLogItem == null)
                    {
                        _currentBatchLogItem = logItem;
                        LstLog.Items.Add(_currentBatchLogItem);
                    }
                    else
                    {
                        _currentBatchLogItem.Content = message;
                        _currentBatchLogItem.Foreground = logItem.Foreground;
                    }
                }
                else
                {
                    _currentBatchLogItem = null;
                    LstLog.Items.Add(logItem);
                }

                if (ChkFollowLog.IsChecked == true && LstLog.Items.Count > 0)
                {
                    LstLog.ScrollIntoView(LstLog.Items[LstLog.Items.Count - 1]);
                }
                
                // Update watchdog - we received activity
                _lastLogUpdate = DateTime.Now;
                _frozenAlertShown = false;
            });
        }

        private static bool ShouldUpdateCurrentBatchLog(string message)
        {
            return message.Contains("Processing batch #", StringComparison.OrdinalIgnoreCase) ||
                   message.Contains("Batch #", StringComparison.OrdinalIgnoreCase) && message.Contains("committed", StringComparison.OrdinalIgnoreCase) ||
                   message.Contains("Total progress:", StringComparison.OrdinalIgnoreCase) ||
                   message.Contains("Processing final batch #", StringComparison.OrdinalIgnoreCase) ||
                   message.Contains("Final batch committed:", StringComparison.OrdinalIgnoreCase);
        }

        private Brush GetLogMessageBrush(string message)
        {
            if (message.Contains("COUNT MATCH", StringComparison.OrdinalIgnoreCase))
            {
                return new SolidColorBrush(Color.FromRgb(30, 215, 96));
            }

            if (message.Contains("COUNT MISMATCH", StringComparison.OrdinalIgnoreCase))
            {
                return new SolidColorBrush(Color.FromRgb(255, 91, 95));
            }

            if (message.Contains("COUNT ACCOUNTED", StringComparison.OrdinalIgnoreCase))
            {
                return new SolidColorBrush(Color.FromRgb(255, 200, 87));
            }

            return new SolidColorBrush(Color.FromRgb(231, 231, 231));
        }

        private void OnTableCompleted(TableMigrationResult result)
        {
            Dispatcher.Invoke(() =>
            {
                var statusIcon = result.ErrorCount > 0 ? "✗" : result.WarningCount > 0 ? "⚠️" : "✓";
                
                var summaryText = $"{statusIcon} {result.TableName}";
                
                if (result.DbfTotalCount.HasValue)
                    summaryText += $"\n   DBF Total: {result.DbfTotalCount.Value:N0}";
                else
                    summaryText += "\n   DBF Total: unknown";

                summaryText += $"\n   Records: {result.RowCount}";
                summaryText += $"\n   Count: {FormatCountStatus(result)}";
                
                if (result.SkippedCount > 0)
                    summaryText += $"\n   Skipped: {result.SkippedCount}";
                
                if (result.DeletedCount > 0)
                    summaryText += $"\n   Deleted: {result.DeletedCount}";
                
                if (result.ErrorCount > 0)
                    summaryText += $"\n   Errors: {result.ErrorCount}";

                if (result.WarningCount > 0)
                    summaryText += $"\n   Warnings: {result.WarningCount}";
                
                summaryText += "\n";

                LstTableSummary.Items.Add(new System.Windows.Controls.ListBoxItem
                {
                    Content = summaryText,
                    Foreground = GetTableSummaryBrush(result)
                });
                if (LstTableSummary.Items.Count > 0)
                {
                    LstTableSummary.ScrollIntoView(LstTableSummary.Items[LstTableSummary.Items.Count - 1]);
                }
            });
        }

        private string FormatCountStatus(TableMigrationResult result)
        {
            return result.CountStatus switch
            {
                "Match" => $"MATCH ({result.RowCount:N0}/{result.DbfTotalCount.GetValueOrDefault():N0})",
                "Accounted" => $"ACCOUNTED ({result.AccountedCount:N0}/{result.DbfTotalCount.GetValueOrDefault():N0})",
                "Mismatch" => $"MISMATCH missing {result.MissingCount:N0} ({result.AccountedCount:N0}/{result.DbfTotalCount.GetValueOrDefault():N0})",
                "SkippedExisting" => "SKIPPED existing MySQL table",
                _ => "unknown"
            };
        }

        private Brush GetTableSummaryBrush(TableMigrationResult result)
        {
            if (result.CountStatus == "Match")
            {
                return new SolidColorBrush(Color.FromRgb(30, 215, 96));
            }

            if (result.ErrorCount > 0 || result.CountStatus == "Mismatch")
            {
                return new SolidColorBrush(Color.FromRgb(255, 91, 95));
            }

            if (result.CountStatus == "Accounted" || result.WarningCount > 0)
            {
                return new SolidColorBrush(Color.FromRgb(255, 200, 87));
            }

            if (result.CountStatus == "SkippedExisting")
            {
                return new SolidColorBrush(Color.FromRgb(167, 167, 167));
            }

            return new SolidColorBrush(Color.FromRgb(231, 231, 231));
        }

        protected override void OnClosing(CancelEventArgs e)
        {
            SaveCurrentSettings();
            base.OnClosing(e);
        }
    }
}
