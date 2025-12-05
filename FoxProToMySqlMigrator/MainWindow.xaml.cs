using System;
using System.Threading;
using System.Windows;
using System.Windows.Media;
using System.Windows.Media.Animation;
using Microsoft.Win32;

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

        public MainWindow()
        {
            InitializeComponent();
            _migrationService = new FoxProMigrationService();
            _migrationService.LogMessage += OnLogMessage;
            _migrationService.TableCompleted += OnTableCompleted;
            
            LoadDefaultSettings();
        }

        private void LoadDefaultSettings()
        {
            TxtMySqlServer.Text = AppSettings.DefaultServerConnection;
            TxtDatabaseName.Text = AppSettings.DefaultDatabaseName;
            TxtBatchSize.Text = "1000";
            ChkSafeMode.IsChecked = AppSettings.DefaultSafeMode;
            ChkSkipDeleted.IsChecked = AppSettings.DefaultSkipDeletedRecords;
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
                TxtCheckpointMessage.Text = $"Found incomplete migration: {checkpoint.CompletedTables.Count} of {checkpoint.TotalTables} tables completed. " +
                                           $"Started: {checkpoint.StartTime:yyyy-MM-dd HH:mm:ss}";
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
            CheckForExistingCheckpoint();
        }

        private void TxtDatabaseName_TextChanged(object sender, System.Windows.Controls.TextChangedEventArgs e)
        {
            CheckForExistingCheckpoint();
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
            }
        }

        private async void BtnResumeCheckpoint_Click(object sender, RoutedEventArgs e)
        {
            if (_currentCheckpoint == null)
                return;

            var result = MessageBox.Show(
                $"Resume migration from checkpoint?\n\n" +
                $"Already completed: {_currentCheckpoint.CompletedTables.Count}/{_currentCheckpoint.TotalTables} tables\n" +
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

            // Check if there's an existing checkpoint and ask user
            if (_currentCheckpoint != null)
            {
                var result = MessageBox.Show(
                    $"Found an incomplete migration:\n\n" +
                    $"Already completed: {_currentCheckpoint.CompletedTables.Count}/{_currentCheckpoint.TotalTables} tables\n" +
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

            await StartMigrationAsync(null);
        }

        private async Task StartMigrationAsync(MigrationCheckpoint? resumeFromCheckpoint)
        {
            try
            {
                _isMigrating = true;
                _cancellationTokenSource = new CancellationTokenSource();
                
                // Update UI
                BtnMigrate.IsEnabled = false;
                BtnStop.Visibility = Visibility.Visible;
                CheckpointNotification.Visibility = Visibility.Collapsed;
                StartSpinner();

                var migrationMode = CmbMigrationMode.SelectedIndex == 0 
                    ? MigrationMode.FullReload 
                    : MigrationMode.PatchLoad;

                // Build connection string with database
                var connectionString = TxtMySqlServer.Text.TrimEnd(';') + $";Database={TxtDatabaseName.Text};";

                // Parse batch size
                int.TryParse(TxtBatchSize.Text, out int batchSize);

                await _migrationService.MigrateAsync(
                    TxtFoxProFolder.Text,
                    connectionString,
                    TxtDatabaseName.Text,
                    ChkSafeMode.IsChecked ?? false,
                    ChkSkipDeleted.IsChecked ?? true,
                    migrationMode,
                    batchSize,
                    resumeFromCheckpoint,
                    _cancellationTokenSource.Token
                );

                if (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    MessageBox.Show("Migration completed successfully!", "Success", MessageBoxButton.OK, MessageBoxImage.Information);
                    _currentCheckpoint = null;
                }
            }
            catch (OperationCanceledException)
            {
                MessageBox.Show("Migration was cancelled. Progress has been saved and you can resume later.", "Cancelled", MessageBoxButton.OK, MessageBoxImage.Warning);
                // Reload checkpoint after cancellation
                CheckForExistingCheckpoint();
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Migration error: {ex.Message}\n\nProgress has been saved.", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                // Reload checkpoint after error
                CheckForExistingCheckpoint();
            }
            finally
            {
                _isMigrating = false;
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
                
                // Restore UI
                BtnMigrate.IsEnabled = true;
                BtnStop.Visibility = Visibility.Collapsed;
                BtnStop.IsEnabled = true;
                StopSpinner();
            }
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
        }

        private void OnLogMessage(string message)
        {
            Dispatcher.Invoke(() =>
            {
                LstLog.Items.Add(message);
                if (LstLog.Items.Count > 0)
                {
                    LstLog.ScrollIntoView(LstLog.Items[LstLog.Items.Count - 1]);
                }
            });
        }

        private void OnTableCompleted(TableMigrationResult result)
        {
            Dispatcher.Invoke(() =>
            {
                var statusIcon = result.ErrorCount > 0 ? "⚠️" : "✓";
                var statusColor = result.ErrorCount > 0 ? Brushes.Orange : Brushes.Green;
                
                var summaryText = $"{statusIcon} {result.TableName}";
                summaryText += $"\n   Records: {result.RowCount}";
                
                if (result.SkippedCount > 0)
                    summaryText += $"\n   Skipped: {result.SkippedCount}";
                
                if (result.DeletedCount > 0)
                    summaryText += $"\n   Deleted: {result.DeletedCount}";
                
                if (result.ErrorCount > 0)
                    summaryText += $"\n   Errors: {result.ErrorCount}";
                
                summaryText += "\n";

                LstTableSummary.Items.Add(summaryText);
                if (LstTableSummary.Items.Count > 0)
                {
                    LstTableSummary.ScrollIntoView(LstTableSummary.Items[LstTableSummary.Items.Count - 1]);
                }
            });
        }
    }
}