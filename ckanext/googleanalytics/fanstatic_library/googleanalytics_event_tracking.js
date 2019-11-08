// Add Google Analytics Event Tracking to resource download links.
this.ckan.module('google-analytics', function(jQuery, _) {
  return {
    options: {
      googleanalytics_resource_prefix: ''
    },
    initialize: function() {
      jQuery("a.resource-url-analytics, a.btn-download").on("click", function() {
          var resource_url = jQuery(this).prop('href');
          if (resource_url) {
            ga('send', 'event', 'Resource', 'Download', resource_url);
          }
      });
   
      jQuery(".dataset-heading a").on("click", function() {
          var dataset_url = jQuery(this).prop('href');
          if (dataset_url) {
            ga('send', 'event', 'Dataset', 'CKAN_Dataset_view', dataset_url);
          }
      });

      jQuery(".dataset-resource-text a:eq(1)").on("click", function() {
          var resource_url = jQuery(this).prop('href');
          if (resource_url) {
            ga('send', 'event', 'Resource', 'CKAN_Resource_view', resource_url);
          }
      });
    
    }
  }
});
